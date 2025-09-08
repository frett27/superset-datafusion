from setuptools import setup, find_packages

setup(
    name="superset-datafusion",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-superset",
        "datafusion-dbapi",
    ],
    entry_points={
        "superset.db_engine_specs": [
            "datafusion = superset_datafusion.engine_spec:DataFusionEngineSpec",
        ],
    },
    python_requires=">=3.8",
)
