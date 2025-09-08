import logging
from superset.db_engine_specs.base import BaseEngineSpec
from superset.db_engine_specs.exceptions import SupersetDBAPIError

logger = logging.getLogger(__name__)

class DataFusionEngineSpec(BaseEngineSpec):
    """DataFusion engine specification for Superset"""
    
    engine = "datafusion"
    engine_name = "Apache DataFusion"
    drivers = {"datafusion_dbapi": "datafusion_dbapi"}
    default_driver = "datafusion_dbapi"
    
    # SQLAlchemy URI format
    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATE_TRUNC('second', {col})",
        "PT1M": "DATE_TRUNC('minute', {col})",
        "PT1H": "DATE_TRUNC('hour', {col})",
        "P1D": "DATE_TRUNC('day', {col})",
        "P1W": "DATE_TRUNC('week', {col})",
        "P1M": "DATE_TRUNC('month', {col})",
        "P1Y": "DATE_TRUNC('year', {col})",
    }
    
    # Allow DML operations (needed for SHOW TABLES, DESCRIBE, etc.)
    allow_dml = True
    
    @classmethod
    def get_schema_names(cls, inspector):
        """Get schema names using SQLAlchemy inspector"""
        logger.info(f"🔍 DataFusionEngineSpec.get_schema_names() called")
        logger.debug(f"   Inspector type: {type(inspector)}")
        logger.debug(f"   Inspector: {inspector}")
        
        try:
            # Use the SQLAlchemy inspector which will use our dialect
            result = inspector.get_schema_names()
            logger.info(f"✅ get_schema_names() success: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ get_schema_names() failed: {e}")
            logger.debug(f"   Exception type: {type(e)}")
            logger.debug(f"   Exception details: {str(e)}")
            # Fallback: return default schema
            fallback_result = ["public"]
            logger.info(f"🔄 get_schema_names() fallback: {fallback_result}")
            return fallback_result
    
    @classmethod
    def get_table_names(cls, database, inspector, schema):
        """Get table names using SQLAlchemy inspector"""
        logger.info(f"🔍 DataFusionEngineSpec.get_table_names() called")
        logger.debug(f"   Database: {database}")
        logger.debug(f"   Database type: {type(database)}")
        logger.debug(f"   Database name: {getattr(database, 'database_name', 'Unknown')}")
        logger.debug(f"   Inspector type: {type(inspector)}")
        logger.debug(f"   Inspector: {inspector}")
        logger.debug(f"   Schema: {schema}")
        
        try:
            # Use the SQLAlchemy inspector which will use our dialect
            result = inspector.get_table_names(schema=schema)
            logger.info(f"✅ get_table_names() success: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ get_table_names() failed: {e}")
            logger.debug(f"   Exception type: {type(e)}")
            logger.debug(f"   Exception details: {str(e)}")
            import traceback
            logger.debug(f"   Traceback: {traceback.format_exc()}")
            # Fallback: return empty list
            fallback_result = []
            logger.info(f"🔄 get_table_names() fallback: {fallback_result}")
            return fallback_result
    
    @classmethod
    def get_columns(cls, database, inspector, table_name, schema):
        """Get column information using SQLAlchemy inspector"""
        logger.info(f"🔍 DataFusionEngineSpec.get_columns() called")
        logger.debug(f"   Database: {database}")
        logger.debug(f"   Inspector type: {type(inspector)}")
        logger.debug(f"   Table name: {table_name}")
        logger.debug(f"   Schema: {schema}")
        
        try:
            # Use the SQLAlchemy inspector which will use our dialect
            result = inspector.get_columns(table_name, schema=schema)
            logger.info(f"✅ get_columns() success: {len(result)} columns")
            logger.debug(f"   Columns: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ get_columns() failed: {e}")
            logger.debug(f"   Exception type: {type(e)}")
            logger.debug(f"   Exception details: {str(e)}")
            # Fallback: return empty list
            fallback_result = []
            logger.info(f"🔄 get_columns() fallback: {fallback_result}")
            return fallback_result
    
    @classmethod
    def convert_dttm(cls, target_type, dttm, db_extra=None):
        """Convert datetime to target type"""
        logger.debug(f"🔍 DataFusionEngineSpec.convert_dttm() called")
        logger.debug(f"   Target type: {target_type}")
        logger.debug(f"   DTTM: {dttm}")
        logger.debug(f"   DB extra: {db_extra}")
        
        result = f"CAST('{dttm}' AS {target_type})"
        logger.debug(f"✅ convert_dttm() result: {result}")
        return result
    
    @classmethod
    def epoch_to_dttm(cls):
        """Convert epoch to datetime"""
        logger.debug(f"🔍 DataFusionEngineSpec.epoch_to_dttm() called")
        
        result = "CAST({col} AS TIMESTAMP)"
        logger.debug(f"✅ epoch_to_dttm() result: {result}")
        return result
