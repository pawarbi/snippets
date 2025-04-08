"""
Sandeep Pawar | fabric.guru
Compares columns between delta tables and T-SQL endpoint in a Fabric lakehouse.
Handles both schema and schemaless lakehouses.
"""

import sempy.fabric as fabric
import polars as pl
import pandas as pd

def compare_delta_tsql_columns(lakehouse_name, lakehouse_workspace):

    lh_workspaceid = fabric.resolve_workspace_id(lakehouse_workspace)
    abfs = notebookutils.lakehouse.getWithProperties(lakehouse_name, lh_workspaceid)['properties']['abfsPath']
    is_lh_withschema = notebookutils.fs.exists(f"{abfs}/Tables/dbo")
    comparison_results = []
    
    def get_table_info(table_name):
        if is_lh_withschema:
            parts = table_name.split('/')
            if len(parts) > 1:
                schema = parts[0]
                table = parts[1]
            else:
                schema = "dbo"
                table = parts[0]
            
            delta_path = f"{abfs}/Tables/{schema}/{table}"
            sql_table_name = f"{schema}.{table}"
        else:
            schema = ""
            table = table_name
            delta_path = f"{abfs}/Tables/{table}"
            sql_table_name = table
        
        return {
            "schema": schema,
            "table": table,
            "delta_path": delta_path,
            "sql_table_name": sql_table_name
        }

    if is_lh_withschema:
        schemas = [f.name for f in notebookutils.fs.ls(f"{abfs}/Tables")]
        tables = []
        
        for schema in schemas:
            try:
                schema_tables = [f"{schema}/{t.name}" for t in notebookutils.fs.ls(f"{abfs}/Tables/{schema}")]
                tables.extend(schema_tables)
            except:
                print(f"Error listing tables in schema: {schema}")
    else:
        tables = [f.name for f in notebookutils.fs.ls(f"{abfs}/Tables")]

    conn = notebookutils.data.connect_to_artifact(lakehouse_name, lh_workspaceid, "Lakehouse")

    for table in tables:
        try:
            table_info = get_table_info(table)
            
            delta_columns = pl.scan_delta(table_info["delta_path"]).collect_schema().names()
            
            if is_lh_withschema:
                sql_query = f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = '{table_info["schema"]}' AND TABLE_NAME = '{table_info["table"]}';
                """
            else:
                sql_query = f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_info["sql_table_name"]}';
                """
            
            tsql_columns = (conn.query(sql_query))['COLUMN_NAME'].to_list()
            
            delta_only = set(delta_columns) - set(tsql_columns)
            tsql_only = set(tsql_columns) - set(delta_columns)
            
            comparison_results.append({
                "table": table_info["sql_table_name"],
                # "delta_path": table_info["delta_path"],
                "delta_only_columns": list(delta_only),
                "tsql_only_columns": list(tsql_only),
                "is_match": len(delta_only) == 0 and len(tsql_only) == 0
            })
            
        except Exception as e:
            print(f"Error processing table {table}: {str(e)}")

    print("\n" + "="*80)
    print(f"COLUMN COMPARISON RESULTS (Found {len(comparison_results)} tables)")
    print("="*80)

    for result in comparison_results:
        print(f"\nTable: {result['table']}")
        
        if result["is_match"]:
            print("✅ Columns match perfectly between Delta and T-SQL")
        else:
            print("❌ Column differences found:")
            
            if result["delta_only_columns"]:
                print(f"  Columns in Delta but missing in T-SQL ({len(result['delta_only_columns'])}):")
                for col in sorted(result["delta_only_columns"]):
                    print(f"    - {col}")
            
            if result["tsql_only_columns"]:
                print(f"  Columns in T-SQL but missing in Delta ({len(result['tsql_only_columns'])}):")
                for col in sorted(result["tsql_only_columns"]):
                    print(f"    - {col}")

    matching_tables = sum(1 for r in comparison_results if r["is_match"])
    print("\n" + "="*80)
    print(f"SUMMARY: {matching_tables} of {len(comparison_results)} tables have matching columns")
    print("="*80)
    
    return comparison_results


compare_delta_tsql_columns("MyLakehouse","Sales")
