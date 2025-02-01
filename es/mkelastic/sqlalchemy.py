import logging
from types import ModuleType
from typing import Any, Dict, List, Optional, Type, Union

from es import basesqlalchemy
import es.elastic
from sqlalchemy import types
from sqlalchemy.engine import Connection
from sqlalchemy.sql import sqltypes

logger = logging.getLogger(__name__)

class ESArray(sqltypes.TypeEngine):
    """Custom SQLAlchemy type for Elasticsearch arrays"""
    
    def __init__(self, item_type: Union[Type[types.TypeEngine], types.TypeEngine]):
        self.item_type = item_type
        super().__init__()

    def __repr__(self):
        return f"ESArray({self.item_type.__class__.__name__})"

class ESCompiler(basesqlalchemy.BaseESCompiler):
    def visit_array(self, array, **kw):
        return f"ARRAY[{array.clauses._compiler_dispatch(self, **kw)}]"

    def visit_array_column(self, column, **kw):
        return self.visit_column(column)

class ESTypeCompiler(basesqlalchemy.BaseESTypeCompiler):
    def visit_ARRAY(self, type_, **kw):
        inner = self.process(type_.item_type)
        return f"ARRAY<{inner}>"

    def visit_ESArray(self, type_, **kw):
        return self.visit_ARRAY(type_)

class ESDialect(basesqlalchemy.BaseESDialect):
    name = "mkelasticsearch"
    scheme = "http"
    driver = "rest"
    statement_compiler = ESCompiler
    type_compiler = ESTypeCompiler

    # Map of ES types to SQLAlchemy types
    _es_to_sa_type = {
        **basesqlalchemy.BaseESDialect._es_to_sa_type,  # Include existing mappings
        "array": ESArray,
        "nested": ESArray,
    }

    @classmethod
    def dbapi(cls) -> ModuleType:
        return es.elastic

    def get_table_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        query = "SHOW VALID_TABLES"
        result = connection.execute(query)
        return [table.name for table in result if table.name[0] != "."]

    def get_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        query = "SHOW VALID_VIEWS"
        result = connection.execute(query)
        return [table.name for table in result if table.name[0] != "."]

    def _get_array_type(self, mapping: str) -> Optional[Type[types.TypeEngine]]:
        """Determine the type of array elements based on ES mapping"""
        if mapping.startswith('array<'):
            inner_type = mapping[6:-1]  # Extract type between array< and >
            base_type = basesqlalchemy.get_type(inner_type)
            return ESArray(base_type)
        return None

    def get_columns(
        self,
        connection: Connection,
        table_name: str,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        # Get regular columns
        query = f'SHOW COLUMNS FROM "{table_name}"'
        all_columns = connection.execute(query)
        
        # Get array columns
        array_columns_result = connection.execute(
            f"SHOW ARRAY_COLUMNS FROM {table_name}"
        ).fetchall()
        
        array_columns = set()
        array_types = {}
        
        # Process array columns and their types
        if array_columns_result:
            for row in array_columns_result:
                col_name = row.name
                array_columns.add(col_name)
                # If type information is available (from enhanced get_array_type_columns)
                if hasattr(row, 'type'):
                    array_types[col_name] = row.type

        columns = []
        for row in all_columns:
            if row.mapping in self._not_supported_column_types:
                continue

            col_info = {
                "name": row.column,
                "nullable": True,
                "default": None,
            }

            if row.column in array_columns:
                # Handle array type
                if row.column in array_types:
                    inner_type = basesqlalchemy.get_type(array_types[row.column])
                else:
                    inner_type = types.String  # Default to string if type unknown
                col_info["type"] = ESArray(inner_type)
            else:
                # Handle regular type
                col_info["type"] = basesqlalchemy.get_type(row.mapping)

            columns.append(col_info)

        return columns

    def _get_array_operators(self):
        """Define array-specific operators for SQLAlchemy"""
        return {
            "array_contains": lambda x, y: f"ARRAY_CONTAINS({x}, {y})",
            "array_length": lambda x: f"ARRAY_LENGTH({x})",
            "array_distinct": lambda x: f"ARRAY_DISTINCT({x})",
        }

ESHTTPDialect = ESDialect

class ESHTTPSDialect(ESDialect):
    scheme = "https"
    default_paramstyle = "pyformat"
