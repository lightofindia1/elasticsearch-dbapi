import re
from typing import Any, Dict, List, Optional, Tuple

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from es import exceptions
from es.baseapi import (
    apply_parameters,
    BaseConnection,
    BaseCursor,
    check_closed,
    CursorDescriptionRow,
    get_description_from_columns,
    Type,
)
from packaging import version


def connect(
    host: str = "localhost",
    port: int = 9200,
    path: str = "",
    scheme: str = "http",
    user: Optional[str] = None,
    password: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> BaseConnection:
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 9200)
        >>> curs = conn.cursor()

    """
    context = context or {}
    return Connection(host, port, path, scheme, user, password, context, **kwargs)


class Connection(BaseConnection):

    """Connection to an ES Cluster"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        path: str = "",
        scheme: str = "http",
        user: Optional[str] = None,
        password: Optional[str] = None,
        context: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            host=host,
            port=port,
            path=path,
            scheme=scheme,
            user=user,
            password=password,
            context=context,
            **kwargs,
        )
        if user and password:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **self.kwargs)
        else:
            self.es = Elasticsearch(self.url, **self.kwargs)

    @check_closed
    def cursor(self) -> BaseCursor:
        """Return a new Cursor Object using the connection."""
        if self.es:
            cursor = Cursor(self.url, self.es, **self.kwargs)
            self.cursors.append(cursor)
            return cursor
        raise exceptions.UnexpectedESInitError()


class ESType(Type):
    """Extended Type enum to support arrays"""
    ARRAY = "ARRAY"
    
    @classmethod
    def from_es_type(cls, es_type: str, is_array: bool = False) -> str:
        """Convert ES type to SQL type"""
        if is_array:
            return cls.ARRAY
        # Existing type mapping logic...
        return cls.STRING  # Default to string for unknown types

def get_description_from_columns_with_arrays(columns: List[Dict[str, Any]]) -> List[CursorDescriptionRow]:
    """Enhanced version of get_description_from_columns that handles arrays"""
    description = []
    for column in columns:
        type_code = ESType.from_es_type(
            column.get("type", "string"),
            is_array="array" in column.get("type", "").lower()
        )
        description.append(
            CursorDescriptionRow(
                column.get("name", ""),
                type_code,
                None,  # display_size
                None,  # internal_size 
                None,  # precision
                None,  # scale
                None,  # null_ok
            )
        )
    return description

class Cursor(BaseCursor):

    """Connection cursor."""

    custom_sql_to_method = {
        "show valid_tables": "get_valid_table_names",
        "show valid_views": "get_valid_view_names",
    }

    def __init__(self, url: str, es: Elasticsearch, **kwargs: Any) -> None:
        super().__init__(url, es, **kwargs)
        self.sql_path = kwargs.get("sql_path") or "_sql"
        self._array_columns: Dict[str, str] = {}

    def _process_array_value(self, value: Any) -> Union[List[Any], Any]:
        """Process array values from ES response"""
        if isinstance(value, list):
            # Handle nested objects in arrays
            if value and isinstance(value[0], dict):
                return [dict(item) for item in value]
            return value
        return value

    def _process_row(self, row: List[Any]) -> Tuple[Any, ...]:
        """Process a row, handling array values"""
        processed = []
        for idx, value in enumerate(row):
            if self.description and idx < len(self.description):
                col_type = self.description[idx].type_code
                if col_type == ESType.ARRAY:
                    processed.append(self._process_array_value(value))
                else:
                    processed.append(value)
        return tuple(processed)

    def _get_value_for_col_name(self, row: Tuple[Any], name: str) -> Any:
        """
        Get the value of a specific column name from a row
        :param row: The result row
        :param name: The column name
        :return: Value
        """
        for idx, col_description in enumerate(self.description):
            if col_description.name == name:
                return row[idx]

    def get_valid_table_view_names(self, type_filter: str) -> "Cursor":
        """
        Custom for "SHOW VALID_TABLES" excludes empty indices from the response
        Mixes `SHOW TABLES` with direct index access info to exclude indexes
        that have no rows so no columns (unless templated). SQLAlchemy will
        not support reflection of tables with no columns

        https://github.com/preset-io/elasticsearch-dbapi/issues/38

        :param: type_filter will filter SHOW_TABLES result by BASE_TABLE or VIEW
        """
        results = self.execute("SHOW TABLES")
        response = self.es.cat.indices(format="json")

        _results = []
        for result in results:
            is_empty = False
            for item in response:
                # First column is TABLE_NAME
                if item["index"] == self._get_value_for_col_name(result, "name"):
                    if int(item["docs.count"]) == 0:
                        is_empty = True
                        break
            if (
                not is_empty
                and self._get_value_for_col_name(result, "type") == type_filter
            ):
                _results.append(result)
        self._results = _results
        return self

    def get_valid_table_names(self) -> "Cursor":
        # Get the ES cluster version. Since 7.10 the table column name changed #52
        cluster_info = self.es.info()
        cluster_version = version.parse(cluster_info["version"]["number"])
        if cluster_version >= version.parse("7.10.0"):
            return self.get_valid_table_view_names("TABLE")
        return self.get_valid_table_view_names("BASE TABLE")

    def get_valid_view_names(self) -> "Cursor":
        return self.get_valid_table_view_names("VIEW")

    @check_closed
    def execute(
        self, operation: str, parameters: Optional[Dict[str, Any]] = None
    ) -> "BaseCursor":
        cursor = self.custom_sql_to_method_dispatcher(operation)
        if cursor:
            return cursor

        re_table_name = re.match("SHOW ARRAY_COLUMNS FROM (.*)", operation)
        if re_table_name:
            return self.get_array_type_columns(re_table_name[1])

        query = apply_parameters(operation, parameters)
        results = self.elastic_query(query)
        # We need a list of tuples
        rows = [self._process_row(row) for row in results.get("rows", [])]
        columns = results.get("columns")
        if not columns:
            raise exceptions.DataError(
                "Missing columns field, maybe it's an opendistro sql ep"
            )
        self._results = rows
        self.description = get_description_from_columns_with_arrays(results["columns"])
        return self

    def get_array_type_columns(self, table_name: str) -> "BaseCursor":
        """Enhanced version that provides more detailed array type information"""
        array_columns: List[Tuple[Any, ...]] = []
        try:
            # Get mapping first to understand array types
            mapping = self.es.indices.get_mapping(index=table_name)
            properties = mapping[table_name]["mappings"].get("properties", {})
            
            # Get a sample document to understand actual data
            response = self.es.search(index=table_name, size=1)
            
            if response["hits"]["total"]["value"] > 0:
                source = response["hits"]["hits"][0]["_source"]
                
                for field_name, field_mapping in properties.items():
                    if isinstance(field_mapping.get("type"), str):
                        # Check if it's an array in the sample document
                        value = source.get(field_name)
                        if isinstance(value, list):
                            array_type = field_mapping.get("type")
                            if isinstance(value[0], dict):
                                # Handle nested object arrays
                                for nested_field in value[0].keys():
                                    array_columns.append(
                                        (f"{field_name}.{nested_field}", array_type)
                                    )
                            else:
                                array_columns.append((field_name, array_type))
                
        except es_exceptions.ConnectionError as e:
            raise exceptions.OperationalError(f"Error connecting to {self.url}: {e.info}")
        except es_exceptions.NotFoundError as e:
            raise exceptions.ProgrammingError(f"Error ({e.error}): {e.info}")
        
        # Update description to include type information
        self.description = [
            CursorDescriptionRow("name", Type.STRING, None, None, None, None, None),
            CursorDescriptionRow("type", Type.STRING, None, None, None, None, None),
        ]
        self._results = array_columns
        return self
