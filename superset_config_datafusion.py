import os
import sys
from superset.config import *

# Add the python directory to Python path for DataFusion modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

# Import DataFusion SQLAlchemy dialect
import sqlalchemy_datafusion

# Import DataFusion Superset engine specification
import superset_datafusion

# MySQL database configuration (matching docker-compose.yml)
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://superset:superset_password@localhost:3306/superset'

# Security settings
SECRET_KEY = 'coZB6ao6+oOmsRdzWyEq0ijGFvnc+dEbG/LHzAokl0FDymKa1Pe+3r2u'

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_ADVANCED_DATA_TYPES": True,
}

# SQLAlchemy engine options
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
    'pool_timeout': 20,
    'max_overflow': 0,
}

# Allow DML operations for DataFusion
ALLOW_DML = True

# Additional configuration
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000
SUPERSET_WEBSERVER_TIMEOUT = 300

# Logging
LOG_LEVEL = 'INFO'

# CORS settings
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# DataFusion specific settings
DATA_FUSION_ALLOW_DML = True


# Static assets configuration - Point to the frontend source assets
STATICFILES_DIRS = [
    os.path.join(os.path.dirname(__file__), 'superset/superset-frontend/src/assets')
]
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(os.path.dirname(__file__), 'superset/superset/static')

# Override the favicon path to point to the source assets
FAVICONS = [{"href": "/static/images/favicon.png"}]
APP_ICON = "/static/images/superset-logo-horiz.png"


# Register custom DataFusion engine spec
from superset_datafusion.engine_spec import DataFusionEngineSpec

# Add DataFusion to custom engine specs
CUSTOM_SECURITY_MANAGER = None

# Register DataFusion engine spec using the correct approach
def INIT_SUPERSET():
    """Initialize custom configurations"""
    try:
        from superset.db_engine_specs import get_engine_spec
        # Force registration by importing our engine spec
        from superset_datafusion.engine_spec import DataFusionEngineSpec
        print("✅ DataFusion engine spec imported successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not register DataFusion engine spec: {e}")

# Call initialization
try:
    INIT_SUPERSET()
except Exception as e:
    print(f"⚠️  Warning: Could not register DataFusion engine spec: {e}")
