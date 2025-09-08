from setuptools import setup, find_packages

setup(
    name="sqlalchemy-datafusion",
    version="0.1.0",
    description="SQLAlchemy dialect for Apache DataFusion",
    author="DataFusion Team",
    packages=find_packages(),
    install_requires=[
        "SQLAlchemy>=1.4,<2.0",
    ],
    python_requires=">=3.7",
    entry_points={
        "sqlalchemy.dialects": [
            "datafusion = sqlalchemy_datafusion.dialect:DataFusionDialect",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
