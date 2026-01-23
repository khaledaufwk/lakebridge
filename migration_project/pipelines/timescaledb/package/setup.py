from setuptools import setup, find_packages

setup(
    name="timescaledb_loader",
    version="2.0.0",
    packages=find_packages(),
    install_requires=[
        "pyyaml",
    ],
    python_requires=">=3.8",
    description="TimescaleDB Incremental Loader for Databricks",
    author="WakeCap Data Team",
)
