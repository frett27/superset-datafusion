from setuptools import setup

setup(
    name="superset-datafusion",
    version="0.1.0",
    packages=["superset_datafusion"],
    package_dir={"superset_datafusion": "superset_datafusion"},
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
