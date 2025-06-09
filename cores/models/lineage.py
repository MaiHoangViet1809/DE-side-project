import typing as t
from pathlib import Path

from sqlglot.lineage import Node
from sqlglot import parse_one, Schema, exp, maybe_parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, traverse_scope
from sqlglot.optimizer.qualify import qualify
from sqlglot.dialects.dialect import DialectType

from pandas import DataFrame, concat

# from cores.utils.providers import ProvideBenchmark


class NodeExtend(Node):
    def get_lineage(self, dialect: DialectType = None):
        nodes = {}
        column_name = None

        for idx, node in enumerate(self.walk()):
            if isinstance(node.expression, exp.Table):
                label = node.name
                formula = None
                sql = f"SELECT {node.name} FROM {node.expression.this}"
                stage = "leaf"
            else:
                # label = node.expression.sql(pretty=True, dialect=dialect)
                # print(["{}.{}".format(column.table, column.name) for column in find_all_in_scope(node.expression, exp.Column)], node.expression, exp.Column)
                list_ref = ["{}.{}".format(column.table, column.name) for column in find_all_in_scope(node.expression, exp.Column)]
                label = list_ref[0] if list_ref else "NULL"
                formula = str(node.expression.this)
                source = node.source.transform(
                    lambda n: (
                        exp.Tag(this=n, prefix="<b>", postfix="</b>") if n is node.expression else n
                    ),
                    copy=False,
                ).sql(dialect=dialect)
                sql = source
                stage = "intermediate" if idx != 0 else "root"
                column_name = label if idx == 0 else column_name

            node_id = id(node)

            nodes[node_id] = {
                "lineage_order": idx,
                "column_name": column_name,
                "stage": stage,
                "node_id": node_id,
                "child_node_id": [id(d) for d in node.downstream],

                "label": label,
                "formula": formula,

                "sql": sql,

            }

        return nodes


def get_column_lineage(
    column: str | exp.Column,
    sql: str | exp.Expression,
    schema: t.Optional[t.Dict | Schema] = None,
    sources: t.Optional[t.Dict[str, str | exp.Subqueryable]] = None,
    dialect: DialectType = None,
    **kwargs,
) -> NodeExtend:
    """Build the lineage graph for a column of a SQL query.

    Args:
        column: The column to build the lineage for.
        sql: The SQL string or expression.
        schema: The schema of tables.
        sources: A mapping of queries which will be used to continue building lineage.
        dialect: The dialect of input SQL.
        **kwargs: Qualification optimizer kwargs.

    Returns:
        A lineage node.
    """
    expression = maybe_parse(sql, dialect=dialect)

    if sources:
        expression = exp.expand(
            expression,
            {
                k: t.cast(exp.Subqueryable, maybe_parse(v, dialect=dialect))
                for k, v in sources.items()
            },
            dialect=dialect,
        )

    qualified = qualify(
        expression,
        dialect=dialect,
        schema=schema,
        **{"validate_qualify_columns": False, "identify": False, **kwargs},  # type: ignore
    )

    scope = build_scope(qualified)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")

    def to_node(
        column: str | int,
        scope: Scope,
        scope_name: t.Optional[str] = None,
        upstream: t.Optional[NodeExtend] = None,
        alias: t.Optional[str] = None,
    ) -> NodeExtend:
        aliases = {
            dt.alias: dt.comments[0].split()[1]
            for dt in scope.derived_tables
            if dt.comments and dt.comments[0].startswith("source: ")
        }

        # Find the specific select clause that is the source of the column we want.
        # This can either be a specific, named select or a generic `*` clause.
        select = (
            scope.expression.selects[column]
            if isinstance(column, int)
            else next(
                (select for select in scope.expression.selects if select.alias_or_name == column),
                exp.Star() if scope.expression.is_star else scope.expression,
            )
        )

        if isinstance(scope.expression, exp.Union):
            upstream = upstream or NodeExtend(name="UNION", source=scope.expression, expression=select)

            index = (
                column
                if isinstance(column, int)
                else next(
                    (
                        i
                        for i, select in enumerate(scope.expression.selects)
                        if select.alias_or_name == column or select.is_star
                    ),
                    -1,  # mypy will not allow a None here, but a negative index should never be returned
                )
            )

            if index == -1:
                raise ValueError(f"Could not find {column} in {scope.expression}")

            for s in scope.union_scopes:
                to_node(index, scope=s, upstream=upstream)

            return upstream

        if isinstance(scope.expression, exp.Select):
            # For better ergonomics in our node labels, replace the full select with
            # a version that has only the column we care about.
            #   "x", SELECT x, y FROM foo
            #     => "x", SELECT x FROM foo
            source = t.cast(exp.Expression, scope.expression.select(select, append=False))
        else:
            source = scope.expression

        # Create the node for this step in the lineage chain, and attach it to the previous one.
        node = NodeExtend(
            name=f"{scope_name}.{column}" if scope_name else str(column),
            source=source,
            expression=select,
            alias=alias or "",
        )

        if upstream:
            upstream.downstream.append(node)

        subquery_scopes = {
            id(subquery_scope.expression): subquery_scope
            for subquery_scope in scope.subquery_scopes
        }

        for subquery in find_all_in_scope(select, exp.Subqueryable):
            subquery_scope = subquery_scopes.get(id(subquery))

            for name in subquery.named_selects:
                to_node(name, scope=subquery_scope, upstream=node)

        # if the select is a star add all scope sources as downstreams
        if select.is_star:
            for source in scope.sources.values():
                if isinstance(source, Scope):
                    source = source.expression
                node.downstream.append(NodeExtend(name=select.sql(), source=source, expression=source))

        # Find all columns that went into creating this one to list their lineage nodes.
        source_columns = set(find_all_in_scope(select, exp.Column))

        # If the source is a UDTF find columns used in the UTDF to generate the table
        if isinstance(source, exp.UDTF):
            source_columns |= set(source.find_all(exp.Column))

        for c in source_columns:
            table = c.table
            source = scope.sources.get(table)

            if isinstance(source, Scope):
                # The table itself came from a more specific scope. Recurse into that one using the unaliased column name.
                to_node(
                    c.name, scope=source, scope_name=table, upstream=node, alias=aliases.get(table)
                )
            else:
                # The source is not a scope - we've reached the end of the line. At this point, if a source is not found
                # it means this column's lineage is unknown. This can happen if the definition of a source used in a query
                # is not passed into the `sources` map.
                source = source or exp.Placeholder()
                node.downstream.append(NodeExtend(name=c.sql(), source=source, expression=source))

        return node

    return to_node(column if isinstance(column, str) else column.name, scope)


# ------------------------------------------------------------------------------
def resolve_path(value: str | Path):
    if isinstance(value, Path):
        value = value.read_text()
    return value


def get_parent(expr):
    if not expr.parent:
        return expr
    elif isinstance(expr, (exp.Join, exp.Where, exp.Group, exp.Order, exp.Having, exp.Select)):
        return expr
    else:
        return get_parent(expr.parent)


def get_root_columns(sql_query, dialect: str = "tsql"):
    ast = parse_one(sql_query, read=dialect)
    ast = qualify(ast, dialect=dialect)

    root = build_scope(ast)

    list_columns = []
    for column in find_all_in_scope(root.expression, exp.Column):
        parent_exp = get_parent(column)
        if not isinstance(parent_exp, (exp.Join, exp.Where, exp.Group, exp.Having)):
            list_columns += [{"name": column.parent.alias_or_name, "source": column.table}]

    return list_columns


# @ProvideBenchmark
def get_lineage(sql: str | Path, dialect: str = "tsql") -> DataFrame:
    sql = resolve_path(sql)

    list_columns = get_root_columns(sql, dialect=dialect)
    nodes = [get_column_lineage(m.get("name"), sql=sql, dialect=dialect).get_lineage(dialect=dialect) for m in list_columns]
    output = concat([DataFrame(node.values()) for node in nodes]).reset_index(drop=True).drop(columns=["sql"])
    return output


def get_source_tables(sql: str | Path, dialect: str = "tsql"):
    sql = resolve_path(sql)

    if "select" not in sql.lower(): return {}

    ast = parse_one(sql, read=dialect)
    ast = qualify(ast, dialect=dialect,
                  validate_qualify_columns=False,
                  )

    tables, ctes = set(), set()
    for cte in ast.find_all(exp.CTE):
        ctes.add(cte.alias_or_name)

    for table_exp in ast.find_all(exp.Table):
        table_name = table_exp.name
        schema = table_exp.db
        full_table_name = (schema + "." + table_name).upper()
        if table_name not in ctes and table_name:
            tables.add(full_table_name)

    return tables - set("")


# UAT section
def parse_test(sql: str | Path, dialect):
    sql = resolve_path(sql)

    ast = parse_one(sql, read=dialect)
    ast = qualify(ast, dialect=dialect, validate_qualify_columns=False)

    tables = get_source_tables(sql, dialect)

    physical_columns = {
        **{m: [] for m in tables},
    }

    for scope in traverse_scope(ast):
        for c in scope.columns:
            if isinstance(scope.sources.get(c.table), (exp.Table, exp.CTE)):
                physical_columns[scope.sources.get(c.table).name.upper()] += [c.name]

    return physical_columns


from diagrams import Diagram
import ast
from diagrams.generic.blank import Blank as Node111
def visualize_column_lineage(df: DataFrame, diagram_name="Column Lineage", out_file="lineage_diagram"):
    """
    Build a diagram from a DataFrame that has columns:
    [node_id, child_node_id, label, ...].
    Each row can connect node_id -> child_node_id (a list).
    """
    # 1. Prepare a dictionary to store Diagrams nodes by node_id
    node_map: dict[int, Node111] = {}

    with Diagram(
        name=diagram_name,
        filename=out_file,
        outformat="png",
        show=False,
        graph_attr={
            "rankdir": "TB",  # top-to-bottom
            "remincross": "true",  # attempt to reduce edge crossing
            "overlap": "false"
        },
    ):
        # 2. Iterate over each row in the DataFrame
        for _, row in df.iterrows():
            node_id = row["node_id"]
            label = str(row["label"])  # or row["column_name"]

            # Create the parent node if not exists
            if node_id not in node_map:
                node_map[node_id] = Node111(label)

            # 3. Parse the child_node_id (which may be a Python list in string form)
            child_str = row["child_node_id"]

            # If it's already a Python list, skip. Otherwise parse the string:
            if isinstance(child_str, str):
                child_ids = ast.literal_eval(child_str)  # safely convert e.g. "[4694696640]" -> [4694696640]
            else:
                child_ids = child_str  # already a list if your DataFrame stores it that way

            # 4. For each child, create the node if needed, and connect
            for child_id in child_ids:
                # If there's no child, e.g. empty list => no edge
                if not child_id:
                    continue

                if child_id not in node_map:
                    # We need a label for the child. Sometimes you have a separate row for that child.
                    # So let's see if we can find it in the DF. We'll do a quick lookup:
                    child_label_rows = df.loc[df["node_id"] == child_id]
                    if not child_label_rows.empty:
                        child_label = str(child_label_rows.iloc[0]["label"])
                    else:
                        # fallback
                        child_label = f"Node {child_id}"

                    node_map[child_id] = Node111(child_label)

                # Now connect
                node_map[node_id] >> node_map[child_id]

    print(f"Diagram saved to {out_file}.png")


if __name__ == "__main__":
    from cores.utils.debug import print_table
    from dotenv import load_dotenv

    load_dotenv()
    df = get_lineage(sql=Path(f"/Users/maihoangviet/Projects/EDW-KC-Project/airflow_home/dags/sql/CONS.TBL_FACT_SALES.sql"), dialect="tsql")
    print_table(df, 1000)
    visualize_column_lineage(df)
