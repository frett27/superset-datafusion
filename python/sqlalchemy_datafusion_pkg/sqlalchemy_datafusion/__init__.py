from .dialect import DataFusionDialect
from sqlalchemy.dialects import registry

registry.register("datafusion", "sqlalchemy_datafusion.dialect", "DataFusionDialect")

__all__ = ["DataFusionDialect"]
