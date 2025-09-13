use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use datafusion::prelude::*;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use tokio::runtime::Runtime;

use std::collections::HashMap;
use url::Url;

fn to_pyerr<E: std::fmt::Display>(e: E) -> PyErr {
    PyException::new_err(e.to_string())
}

/// Convert Arrow DataType to a crude DB-API "type_code" (int).
fn arrow_dtype_to_dbapi_typecode(dt: &DataType) -> i32 {
    match dt {
        DataType::Int64 | DataType::UInt64 => 1,
        DataType::Int32 | DataType::UInt32 => 2,
        DataType::Float64 => 3,
        DataType::Float32 => 4,
        DataType::Boolean => 5,
        DataType::Utf8 | DataType::LargeUtf8 => 6,
        _ => 0,
    }
}

fn arrow_value_at(py: Python<'_>, arr: &dyn Array, idx: usize) -> PyResult<PyObject> {
    if arr.is_null(idx) {
        return Ok(py.None());
    }
    macro_rules! downcast {
        ($T:ty, $getter:ident) => {
            if let Some(a) = arr.as_any().downcast_ref::<$T>() {
                let v = a.$getter(idx);
                return Ok(v.into_py(py));
            }
        };
    }
    // integers
    downcast!(Int8Array, value);
    downcast!(Int16Array, value);
    downcast!(Int32Array, value);
    downcast!(Int64Array, value);
    downcast!(UInt8Array, value);
    downcast!(UInt16Array, value);
    downcast!(UInt32Array, value);
    downcast!(UInt64Array, value);
    // floats
    downcast!(Float32Array, value);
    downcast!(Float64Array, value);
    // bool
    downcast!(BooleanArray, value);
    // utf8
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(a.value(idx).to_string().into_py(py));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(a.value(idx).to_string().into_py(py));
    }
    // fallback: debug
    Ok(format!("{:?}", format!("{:?}", arr)).into_py(py))
}

fn record_batch_row_to_py_tuple(py: Python<'_>, batch: &RecordBatch, row: usize) -> PyResult<PyObject> {
    let mut vals: Vec<PyObject> = Vec::with_capacity(batch.num_columns());
    for col in 0..batch.num_columns() {
        let arr = batch.column(col).as_ref();
        vals.push(arrow_value_at(py, arr, row)?);
    }
    Ok(PyTuple::new(py, vals).into())
}

#[pyclass]
pub struct DFConnection {
    rt: Arc<Runtime>,
    ctx: SessionContext,
    registered_tables: Arc<Mutex<HashSet<String>>>,
}

#[pyclass]
pub struct DFCursor {
    rt: Arc<Runtime>,
    ctx: SessionContext,
    last_batches: Vec<RecordBatch>,
    rowcount: isize,
    pos: usize,
    // (name, type_code)
    desc_simple: Option<Vec<(String, i32)>>,
}

#[pymethods]
impl DFConnection {
    /// Return a new cursor (independent iterator over results).
    fn cursor(&self) -> PyResult<DFCursor> {
        Ok(DFCursor {
            rt: Arc::clone(&self.rt),
            ctx: self.ctx.clone(),
            last_batches: vec![],
            rowcount: -1,
            pos: 0,
            desc_simple: None,
        })
    }

    /// No-op for DB-API compliance.
    fn commit(&self) -> PyResult<()> { Ok(()) }
    fn rollback(&self) -> PyResult<()> { Ok(()) }
    fn close(&self) -> PyResult<()> { Ok(()) }

    /// Convenience helper: register a Parquet file as a table.
    fn register_parquet(&mut self, table: &str, path: &str) -> PyResult<()> {
        let ctx = self.ctx.clone();
        let mut registered_tables = self.registered_tables.lock().unwrap();
        if registered_tables.contains(table) {
            log::info!("table {} already registered", table);
            return Ok(());
        }
        self.rt.block_on(async move {
            ctx.register_parquet(table, path, ParquetReadOptions::default())
                .await
                .map_err(to_pyerr)
        })?;
        registered_tables.insert(table.to_string());
        Ok(())
    }

    /// Convenience helper: register a CSV file as a table.
    fn register_csv(&self, table: &str, path: &str, has_header: Option<bool>) -> PyResult<()> {
        let mut registered_tables = self.registered_tables.lock().unwrap();
        if registered_tables.contains(table) {
            return Ok(());
        }

        let ctx = self.ctx.clone();
        let opts = CsvReadOptions::default().has_header(has_header.unwrap_or(true));

        self.rt.block_on(async move {
            log::debug!("registering csv file: {}", path);
            ctx.register_csv(table, path, opts)
               .await
               .map_err(to_pyerr)
        })?;
        registered_tables.insert(table.to_string());
        Ok(())
    }
}

#[pymethods]
impl DFCursor {
    /// DB-API .description: list of 7-tuples
    #[getter]
    fn description<'py>(&self, py: Python<'py>) -> PyResult<Option<Vec<&'py PyTuple>>> {
        if let Some(desc) = &self.desc_simple {
            let mut out = Vec::with_capacity(desc.len());
            for (name, type_code) in desc {
                out.push(PyTuple::new(py, vec![
                    name.into_py(py),
                    (*type_code).into_py(py),
                    py.None(), py.None(), py.None(), py.None(), py.None(),
                ]));
            }
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }

    #[getter]
    fn rowcount(&self) -> PyResult<isize> { Ok(self.rowcount) }

    fn close(&mut self) -> PyResult<()> { Ok(()) }

    /// Execute a SQL string. (Parameter binding not supported.)
    fn execute(&mut self, sql: &str, params: Option<Vec<PyObject>>) -> PyResult<()> {
        if params.is_some() {
            return Err(PyException::new_err("Parameter binding not supported; interpolate on caller side."));
        }
        let ctx = self.ctx.clone();
        let (batches, schema) = self.rt.block_on(async move {
            let df = ctx.sql(sql).await.map_err(to_pyerr)?;
            let schema = df.schema().clone();
            let batches = df.collect().await.map_err(to_pyerr)?;
            Ok::<_, PyErr>((batches, schema))
        })?;

        self.desc_simple = Some(schema.fields().iter()
            .map(|f| (f.name().clone(), arrow_dtype_to_dbapi_typecode(f.data_type())))
            .collect());

        self.rowcount = batches.iter().map(|b| b.num_rows() as isize).sum();
        self.pos = 0;
        self.last_batches = batches;
        Ok(())
    }

    fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if let Some(row) = self.next_row(py)? { Ok(Some(row)) } else { Ok(None) }
    }

    fn fetchmany(&mut self, py: Python<'_>, size: Option<usize>) -> PyResult<Vec<PyObject>> {
        let n = size.unwrap_or(1);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(row) = self.next_row(py)? { out.push(row); } else { break; }
        }
        Ok(out)
    }

    fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let mut out = Vec::new();
        while let Some(row) = self.next_row(py)? { out.push(row); }
        Ok(out)
    }
}

impl DFCursor {
    fn next_row(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let mut remaining = self.pos;
        for batch in &self.last_batches {
            if remaining < batch.num_rows() {
                let row = record_batch_row_to_py_tuple(py, batch, remaining)?;
                self.pos += 1;
                return Ok(Some(row));
            }
            remaining -= batch.num_rows();
        }
        Ok(None)
    }
}

#[pyfunction]
fn connect(dsn: Option<&str>) -> PyResult<DFConnection> {
    log::debug!("Connecting to DataFusion");
    let rt = Arc::new(Runtime::new().map_err(to_pyerr)?);
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    
    // Parse connection parameters from DSN if provided
    // this is the path to a sql datafusion path that defines the tables to be used
    if let Some(dsn_str) = dsn {
        log::debug!("DSN provided: {}", dsn_str);
        let sql_file = parse_connection_url(dsn_str)?;
    
    
        // Auto-register data files from the specified directory
        log::debug!("Registering data files from SQL file: {}", sql_file);
        register_data_files(&rt, &ctx, &sql_file)?;
    
    }
    
    Ok(DFConnection { 
        rt, 
        ctx, 
        registered_tables: Arc::new(Mutex::new(HashSet::new())) 
    })
}

/// Parse connection URL and extract parameters
fn parse_connection_url(dsn: &str) -> PyResult<String> {
    log::debug!("Parsing connection URL: {}", dsn);
    
    let trimmed = dsn.trim();
    if trimmed.is_empty() {
        return Err(PyException::new_err("Connection string cannot be empty"));
    }
    
    // Try to parse as a full URL first
    if let Ok(url) = Url::parse(trimmed) {
        // If it's a datafusion:// URL, extract the path
        if url.scheme() == "datafusion" {
            let database = url.path().trim_start_matches('/');
            if database.is_empty() {
                return Err(PyException::new_err("Database path is required in connection URL"));
            }
            
            // URL decode the path
            let decoded_path = urlencoding::decode(database)
                .map_err(|e| PyException::new_err(format!("Failed to decode URL path: {}", e)))?;
            
            // Parse query parameters for additional configuration
            let mut params = HashMap::new();
            for (key, value) in url.query_pairs() {
                params.insert(key.to_string(), value.to_string());
            }
            
            log::debug!("Connection URL parsed - Database: {}", decoded_path);
            if !params.is_empty() {
                log::debug!("Query parameters: {:?}", params);
            }
            
            return Ok(decoded_path.to_string());
        } else {
            // Wrong scheme - treat as file path
            log::debug!("Wrong scheme '{}', treating as file path: {}", url.scheme(), trimmed);
            return Ok(trimmed.to_string());
        }
    }
    
    // If URL parsing fails, treat the entire string as a file path
    log::debug!("URL parsing failed, treating as file path: {}", trimmed);
    Ok(trimmed.to_string())
}

/// Register data files and prepare context from SQL file
fn register_data_files(rt: &Arc<Runtime>, ctx: &SessionContext, sql_file: &str) -> PyResult<()> {
    log::debug!("Reading SQL file: {}", sql_file);
    
    // Read the SQL file content
    let sql_content = std::fs::read_to_string(sql_file)
        .map_err(|e| PyException::new_err(format!("Failed to read SQL file '{}': {}", sql_file, e)))?;
    
    // Split SQL content into individual statements
    let statements: Vec<&str> = sql_content
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    
    log::debug!("Found {} SQL statements to execute", statements.len());
    
    // Execute each SQL statement
    for (i, statement) in statements.iter().enumerate() {
        if statement.is_empty() {
            continue;
        }
        
        log::info!("Executing statement {}: {}", i + 1, statement);
        
        let ctx_clone = ctx.clone();
        rt.block_on(async move {
            // Execute the SQL statement
            ctx_clone.sql(statement).await
        }).map_err(|e| PyException::new_err(format!("Failed to execute SQL statement {}: {}", i + 1, e)))?;
        
        log::debug!("Successfully executed statement {}", i + 1);
    }
    
    log::debug!("Successfully prepared DataFusion context from SQL file");
    Ok(())
}

#[pymodule]
fn datafusion_dbapi(py: Python, m: &PyModule) -> PyResult<()> {

    let _ = env_logger::builder().filter_level(log::LevelFilter::Info).try_init();
    log::debug!("Initializing datafusion_dbapi module");
    // DB-API required module attributes
    m.add("apilevel", "2.0")?;
    m.add("threadsafety", 1)?; // 1: Threads may share the module
    m.add("paramstyle", "qmark")?;

    m.add_class::<DFConnection>()?;
    m.add_class::<DFCursor>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    // DB-API Error classes - properly inheriting from BaseException
    m.add_class::<Error>()?;
    m.add_class::<Warning>()?;
    m.add_class::<InterfaceError>()?;
    m.add_class::<DatabaseError>()?;
    m.add_class::<DataError>()?;
    m.add_class::<OperationalError>()?;
    m.add_class::<IntegrityError>()?;
    m.add_class::<InternalError>()?;
    m.add_class::<ProgrammingError>()?;
    m.add_class::<NotSupportedError>()?;

    // Expose helper aliases for Python-land convenience
    // so users can do: import datafusion_dbapi as df; conn = df.connect(); conn.register_parquet(...)
    Ok(())
}

// DB-API 2.0 Error classes - properly inheriting from BaseException
#[pyclass(extends=PyException)]
pub struct Error {
    message: String,
}

#[pymethods]
impl Error {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct Warning {
    message: String,
}

#[pymethods]
impl Warning {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct InterfaceError {
    message: String,
}

#[pymethods]
impl InterfaceError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct DatabaseError {
    message: String,
}

#[pymethods]
impl DatabaseError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct DataError {
    message: String,
}

#[pymethods]
impl DataError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct OperationalError {
    message: String,
}

#[pymethods]
impl OperationalError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct IntegrityError {
    message: String,
}

#[pymethods]
impl IntegrityError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct InternalError {
    message: String,
}

#[pymethods]
impl InternalError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct ProgrammingError {
    message: String,
}

#[pymethods]
impl ProgrammingError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[pyclass(extends=PyException)]
pub struct NotSupportedError {
    message: String,
}

#[pymethods]
impl NotSupportedError {
    #[new]
    fn new(message: String) -> PyResult<Self> {
        Ok(Self { message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    // Helper function to test URL parsing without Python runtime
    fn test_url_parsing(input: &str) -> Result<String, String> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err("Connection string cannot be empty".to_string());
        }
        
        // Try to parse as a full URL first
        if let Ok(url) = Url::parse(trimmed) {
            // If it's a datafusion:// URL, extract the path
            if url.scheme() == "datafusion" {
                let database = url.path().trim_start_matches('/');
                if database.is_empty() {
                    return Err("Database path is required in connection URL".to_string());
                }
                // URL decode the path
                let decoded_path = urlencoding::decode(database)
                    .map_err(|e| format!("Failed to decode URL path: {}", e))?;
                return Ok(decoded_path.to_string());
            } else {
                // Wrong scheme - treat as file path
                return Ok(trimmed.to_string());
            }
        }
        
        // If URL parsing fails, treat the entire string as a file path
        Ok(trimmed.to_string())
    }

    #[test]
    fn test_valid_datafusion_urls() {
        // Valid datafusion:// URLs
        assert_eq!(test_url_parsing("datafusion://localhost/path/to/data").unwrap(), "path/to/data");
        assert_eq!(test_url_parsing("datafusion://user:pass@host:8080/database").unwrap(), "database");
        assert_eq!(test_url_parsing("datafusion:///absolute/path").unwrap(), "absolute/path");
        assert_eq!(test_url_parsing("datafusion://host/db?param=value").unwrap(), "db");
        assert_eq!(test_url_parsing("datafusion://localhost:5432/my_database?timeout=30&retries=3").unwrap(), "my_database");
    }

    #[test]
    fn test_invalid_datafusion_urls() {
        // Invalid datafusion:// URLs
        assert!(test_url_parsing("datafusion://").is_err());
        assert!(test_url_parsing("datafusion://localhost").is_err());
        assert!(test_url_parsing("datafusion://localhost/").is_err());
    }

    #[test]
    fn test_wrong_scheme_urls() {
        // URLs with wrong schemes should be treated as file paths
        assert_eq!(test_url_parsing("postgresql://localhost/db").unwrap(), "postgresql://localhost/db");
        assert_eq!(test_url_parsing("mysql://user:pass@host/db").unwrap(), "mysql://user:pass@host/db");
        assert_eq!(test_url_parsing("http://example.com/path").unwrap(), "http://example.com/path");
    }

    #[test]
    fn test_file_paths() {
        // File paths (should be treated as file paths, not URLs)
        assert_eq!(test_url_parsing("/absolute/path/to/file.sql").unwrap(), "/absolute/path/to/file.sql");
        assert_eq!(test_url_parsing("./relative/path.sql").unwrap(), "./relative/path.sql");
        assert_eq!(test_url_parsing("../parent/path.sql").unwrap(), "../parent/path.sql");
        assert_eq!(test_url_parsing("file.sql").unwrap(), "file.sql");
        assert_eq!(test_url_parsing("data/setup.sql").unwrap(), "data/setup.sql");
    }

    #[test]
    fn test_edge_cases() {
        // Edge cases
        assert!(test_url_parsing("").is_err());
        assert!(test_url_parsing("   ").is_err());
        assert_eq!(test_url_parsing("datafusion://host/path with spaces").unwrap(), "path with spaces");
        assert_eq!(test_url_parsing("datafusion://host/path%20with%20encoding").unwrap(), "path with encoding");
    }

    #[test]
    fn test_malformed_urls() {
        // Malformed URLs that should be treated as file paths
        assert_eq!(test_url_parsing("not-a-url").unwrap(), "not-a-url");
        assert_eq!(test_url_parsing("://invalid").unwrap(), "://invalid");
        assert!(test_url_parsing("datafusion://").is_err());
    }

    #[test]
    fn test_query_parameters() {
        // Test that query parameters are parsed but don't affect the path
        let result = test_url_parsing("datafusion://host/database?param1=value1&param2=value2");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "database");
    }

    #[test]
    fn test_url_parsing_edge_cases() {
        // Test specific edge cases that might cause issues
        assert_eq!(test_url_parsing("datafusion://host/path").unwrap(), "path");
        assert_eq!(test_url_parsing("datafusion://host/path/").unwrap(), "path/");
        assert_eq!(test_url_parsing("datafusion://host/path/subpath").unwrap(), "path/subpath");
        
        // Test with special characters
        assert_eq!(test_url_parsing("datafusion://host/path-with-dashes").unwrap(), "path-with-dashes");
        assert_eq!(test_url_parsing("datafusion://host/path_with_underscores").unwrap(), "path_with_underscores");
        assert_eq!(test_url_parsing("datafusion://host/path.with.dots").unwrap(), "path.with.dots");
    }
}


