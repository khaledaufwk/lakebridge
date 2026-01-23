"""
Credentials management for Lakebridge migrations.

Handles loading, validating, and accessing credentials for SQL Server
and Databricks connections.
"""

import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class SQLServerCredentials:
    """SQL Server connection credentials."""
    server: str
    database: str
    user: str
    password: str
    port: int = 1433
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: bool = True
    trust_server_certificate: bool = False

    @property
    def connection_string(self) -> str:
        """Generate ODBC connection string."""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Encrypt={'yes' if self.encrypt else 'no'};"
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
        )

    @property
    def jdbc_url(self) -> str:
        """Generate JDBC URL for Databricks."""
        return (
            f"jdbc:sqlserver://{self.server}:{self.port};"
            f"database={self.database};"
            f"encrypt={'true' if self.encrypt else 'false'};"
            f"trustServerCertificate={'true' if self.trust_server_certificate else 'false'}"
        )


@dataclass
class DatabricksCredentials:
    """Databricks workspace credentials."""
    host: str
    token: str
    catalog: str
    schema: str

    @property
    def workspace_url(self) -> str:
        """Get the full workspace URL."""
        if self.host.startswith("https://"):
            return self.host
        return f"https://{self.host}"


@dataclass
class RequiredLibrary:
    """A library required for notebook execution."""
    name: str
    type: str  # "whl", "pypi", "jar"
    path: Optional[str] = None  # For whl/jar
    package: Optional[str] = None  # For pypi
    preinstalled: bool = False  # True if library is pre-installed on Databricks


@dataclass
class ComputeConfig:
    """Compute/cluster configuration for notebook execution."""
    cluster_name: str
    cluster_id: Optional[str] = None
    required_libraries: Optional[list] = None

    def get_required_libraries(self) -> list:
        """Get required libraries as list of dicts for DatabricksClient."""
        if not self.required_libraries:
            return []
        return [
            {
                "name": lib.name,
                "type": lib.type,
                "path": lib.path,
                "package": lib.package,
                "preinstalled": lib.preinstalled,
            }
            for lib in self.required_libraries
        ]


class CredentialsManager:
    """
    Manages credentials for Lakebridge migrations.

    Loads credentials from ~/.databricks/labs/lakebridge/.credentials.yml
    and provides validated access to SQL Server and Databricks configs.

    Usage:
        creds = CredentialsManager()
        creds.load()

        # Access SQL Server credentials
        print(creds.sqlserver.jdbc_url)

        # Access Databricks credentials
        print(creds.databricks.workspace_url)
    """

    DEFAULT_PATH = Path.home() / ".databricks" / "labs" / "lakebridge" / ".credentials.yml"

    def __init__(self, path: Optional[Path] = None):
        """Initialize with optional custom credentials path."""
        self.path = path or self.DEFAULT_PATH
        self._config: dict = {}
        self._sqlserver: Optional[SQLServerCredentials] = None
        self._databricks: Optional[DatabricksCredentials] = None
        self._compute: Optional[ComputeConfig] = None

    def load(self) -> "CredentialsManager":
        """Load credentials from YAML file."""
        if not self.path.exists():
            raise FileNotFoundError(
                f"Credentials file not found: {self.path}\n"
                f"Create it with SQL Server and Databricks credentials."
            )

        with open(self.path, "r") as f:
            self._config = yaml.safe_load(f)

        self._parse_sqlserver()
        self._parse_databricks()
        self._parse_compute()

        return self

    def _parse_sqlserver(self) -> None:
        """Parse SQL Server credentials from config."""
        mssql = self._config.get("mssql", {})

        required = ["server", "database", "user", "password"]
        missing = [k for k in required if k not in mssql]
        if missing:
            raise ValueError(f"Missing SQL Server credentials: {missing}")

        self._sqlserver = SQLServerCredentials(
            server=mssql["server"],
            database=mssql["database"],
            user=mssql["user"],
            password=mssql["password"],
            port=mssql.get("port", 1433),
            driver=mssql.get("driver", "ODBC Driver 18 for SQL Server"),
            encrypt=mssql.get("encrypt", True),
            trust_server_certificate=mssql.get("trustServerCertificate", False),
        )

    def _parse_databricks(self) -> None:
        """Parse Databricks credentials from config."""
        db = self._config.get("databricks", {})

        required = ["host", "token", "catalog", "schema"]
        missing = [k for k in required if k not in db]
        if missing:
            raise ValueError(f"Missing Databricks credentials: {missing}")

        self._databricks = DatabricksCredentials(
            host=db["host"],
            token=db["token"],
            catalog=db["catalog"],
            schema=db["schema"],
        )

    def _parse_compute(self) -> None:
        """Parse compute/cluster configuration from config."""
        compute = self._config.get("compute", {})

        if not compute:
            # Compute config is optional
            self._compute = None
            return

        # Parse required libraries
        required_libs = []
        for lib_config in compute.get("required_libraries", []):
            required_libs.append(RequiredLibrary(
                name=lib_config.get("name", "unknown"),
                type=lib_config.get("type", "whl"),
                path=lib_config.get("path"),
                package=lib_config.get("package"),
                preinstalled=lib_config.get("preinstalled", False),
            ))

        self._compute = ComputeConfig(
            cluster_name=compute.get("cluster_name", ""),
            cluster_id=compute.get("cluster_id"),
            required_libraries=required_libs if required_libs else None,
        )

    @property
    def sqlserver(self) -> SQLServerCredentials:
        """Get SQL Server credentials."""
        if self._sqlserver is None:
            raise RuntimeError("Credentials not loaded. Call load() first.")
        return self._sqlserver

    @property
    def databricks(self) -> DatabricksCredentials:
        """Get Databricks credentials."""
        if self._databricks is None:
            raise RuntimeError("Credentials not loaded. Call load() first.")
        return self._databricks

    @property
    def compute(self) -> Optional[ComputeConfig]:
        """Get compute/cluster configuration (may be None if not configured)."""
        return self._compute

    @property
    def secret_scope(self) -> str:
        """Get the default secret scope name."""
        return self._config.get("secret_scope", "migration_secrets")

    def validate(self) -> list[str]:
        """
        Validate credentials and return list of issues.

        Returns empty list if all credentials are valid.
        """
        issues = []

        if not self.path.exists():
            issues.append(f"Credentials file not found: {self.path}")
            return issues

        try:
            self.load()
        except Exception as e:
            issues.append(f"Failed to load credentials: {e}")
            return issues

        # Validate SQL Server
        if not self._sqlserver.server:
            issues.append("SQL Server hostname is empty")
        if not self._sqlserver.database:
            issues.append("SQL Server database name is empty")

        # Validate Databricks
        if not self._databricks.host:
            issues.append("Databricks host is empty")
        if not self._databricks.token:
            issues.append("Databricks token is empty")
        if len(self._databricks.token) < 20:
            issues.append("Databricks token appears to be invalid (too short)")

        return issues

    def to_dict(self) -> dict:
        """Export credentials as dictionary (for debugging, masks passwords)."""
        return {
            "sqlserver": {
                "server": self._sqlserver.server if self._sqlserver else None,
                "database": self._sqlserver.database if self._sqlserver else None,
                "user": self._sqlserver.user if self._sqlserver else None,
                "password": "***masked***",
            },
            "databricks": {
                "host": self._databricks.host if self._databricks else None,
                "catalog": self._databricks.catalog if self._databricks else None,
                "schema": self._databricks.schema if self._databricks else None,
                "token": "***masked***",
            },
        }
