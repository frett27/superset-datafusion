"""
Superset DataFusion Plugin
"""

from superset import app

# Register DataFusion engine specification when the app starts
@app.before_first_request
def register_datafusion_engine():
    """Register DataFusion engine specification with Superset"""
    try:
        from superset_datafusion.engine_spec import DataFusionEngineSpec
        from superset.db_engine_specs import load_engine_specs
        
        # Load engine specs to register DataFusion
        load_engine_specs()
        print("DataFusion engine specification registered successfully")
    except Exception as e:
        print(f"Failed to register DataFusion engine specification: {e}")

__version__ = "0.1.0"
