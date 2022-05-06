"""
Utility to read and write from and to mysql
"""
from builtins import str
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, Any, Union

import MySQLdb
import pandas.io.sql as sql
from MySQLdb.cursors import BaseCursor
from pandas import DataFrame

from hip_data_tools.common import SecretsManager, KeyValueSource, ENVIRONMENT, LOG


def prepare_upsert_query(table: str, primary_keys: Dict[str, Any],
    data: Dict[str, Any]) -> Union[str, Tuple]:
    """
    Helper to prep upsert query
    Args:
        table (str): the upsert statement will be prepared for this table
        primary_keys (Dict[str, Any]): a dictionary of primary key columns and their values
        data (Dict[str, Any]): a dictionary of non primary key columns and their values

    Returns: str, Tuple

    """
    pk_columns = primary_keys.keys()
    data_columns = data.keys()
    pk_col_list = ", ".join(pk_columns)
    data_col_list = ", ".join(data_columns)
    parameter_list = ",".join(["%s" for _ in pk_columns] + ["%s" for _ in data_columns])
    update_list = ", ".join([f"{col}=%s" for col in data_columns])
    values = tuple(list(primary_keys.values()) + list(data.values()))
    query = f"""
    INSERT INTO {table} ({pk_col_list}, {data_col_list})
    VALUES ({parameter_list})
    ON DUPLICATE KEY UPDATE {update_list}
    """

    return query, values


class MySqlSecretsManager(SecretsManager):
    """
    Secrets manager for MySql configuration
    Args:
        source (KeyValueSource): a kv source that has secrets
        username_var (str): the variable name or the key for finding username
        password_var (str): the variable name or the key for finding password
    """

    def __init__(self, source: KeyValueSource = ENVIRONMENT,
                 username_var: str = "MYSQL_USERNAME",
                 password_var: str = "MYSQL_PASSWORD"):
        super().__init__([username_var, password_var], source)

        self.username = self.get_secret(username_var)
        self.password = self.get_secret(password_var)


@dataclass
class MySqlConnectionSettings:
    """Encapsulates the MySql connection settings"""
    host: str
    port: int
    schema: str
    secrets_manager: MySqlSecretsManager
    ssl: Optional[Dict] = None
    local_infile: Optional[int] = None
    charset: Optional[str] = None
    cursor_class: Optional[BaseCursor] = None


class MySqlConnectionManager:
    """
    Creates and manages connection to the mysql database.
    Example -

    Args:
        settings (MySqlConnectionSettings): settings to use for connecting to a database
    """

    def __init__(self, settings: MySqlConnectionSettings):
        self._settings = settings
        self._conn_config = {}

    def get_conn(self):
        """
        Get mysql connection

        Returns: MySql connection object

        """
        self._prepare_connection_config()
        return MySQLdb.connect(**self._conn_config)

    def _prepare_connection_config(self):
        self._conn_config = {
            "user": self._settings.secrets_manager.username,
            "passwd": self._settings.secrets_manager.password,
            "host": self._settings.host,
            "port": self._settings.port,
            "db": self._settings.schema,
        }
        self._set_conn_charset()
        self._set_conn_cursor_class()
        self._set_conn_ssl()
        self._set_conn_local_infile()

    def _set_conn_local_infile(self):
        if self._settings.local_infile:
            self._conn_config["local_infile"] = 1

    def _set_conn_ssl(self):
        if self._settings.ssl:
            self._conn_config['ssl'] = self._settings.ssl

    def _set_conn_cursor_class(self):
        if self._settings.cursor_class:
            self._conn_config["cursorclass"] = self._settings.cursor_class

    def _set_conn_charset(self):
        if self._settings.charset:
            self._conn_config["charset"] = self._settings.charset
            if self._conn_config["charset"].lower() in ['utf8', 'utf-8']:
                self._conn_config["use_unicode"] = True


class MySqlUtil:
    """
    Utility to connect to and query from and to MySql

    Args:
        conn (MySqlConnectionManager): handles connection lifecycles to mysql
    """

    def __init__(self, conn: MySqlConnectionManager):
        self._conn_mgr = conn

    def select_dataframe(self, query) -> DataFrame:
        """
        execute SQL query using Airflow mysql hook and retrieve data in pandas data frame

        Args:
            query (str): Mysql compliant query string

        Returns: DataFrame

        """
        LOG.info("Executing \n %s", query)
        with closing(self._conn_mgr.get_conn()) as connection:
            df = sql.read_sql(query, connection)
        LOG.info("Sql Data frame size: %s", df.shape[0])
        LOG.debug(df.head(2))
        return df

    def upsert(self, table: str, primary_keys: Dict[str, Any], data: Dict[str, Any]) -> None:
        """
        upsert data into a table
        Args:
            table (str): the upsert statement will be prepared for this table
            primary_keys (Dict[str, Any]): a dictionary of primary key columns and their values
            data (Dict[str, Any]): a dictionary of non primary key columns and their values

        Returns: None

        """
        query, values = prepare_upsert_query(table=table, primary_keys=primary_keys, data=data)
        self.execute(query=query, values=values)

    def execute(self, query: str, values: Optional[Tuple] = None) -> None:
        """
        execute standalone SQL statements in a mysql database
        Args:
            query (str): Mysql compliant query string
            values (Tuple): n tuple for substituting values in the query

        Returns: None

        """
        LOG.info("Executing \n %s", query)
        conn = self._conn_mgr.get_conn()
        cur = conn.cursor()
        if values:
            cur.execute(query, values)
        else:
            cur.execute(query)
        conn.commit()

    def get_table_metadata(self, table: str) -> Dict:
        """
        Retrieve a dictionary of table metadata from mysql

        Args:
            table (str): Mysql table name

        Returns: Dict

        """
        query = f"SHOW columns FROM {table}"
        column_df = self.select_dataframe(query=query)
        return column_df.to_dict('records')
