%pip install semantic-link-labs --q

import pandas as pd
import sempy_labs as labs
import pyarrow.parquet as pq
import numpy as np
import sempy.fabric as fabric
import os
import pyarrow.dataset as ds
from datetime import datetime
import fsspec 
import notebookutils
from concurrent.futures import ThreadPoolExecutor

def gather_table_details(workspace=None, lakewarehouse=None, countrows=True):
    """
    Author : Sandeep Pawar | Fabric.Guru  |  September 13, 2024
    
    Gathers and returns detailed information about delta tables in a specified workspace and lakehouse/warehouse.
    You will hit Fabric API rate limits if you scan too many times and get ClientAuthenticationError

    Requirements : Fabric Runtime >= 1.2, Contributor access to the items, semantic-link-labs installed

    Args:
        workspace (str): Workspace ID or name. Defaults to the workspace attached to the notebook if None.
        lakewarehouse (str): Lakehouse or Warehouse ID/name. If not provided, the default lakehouse is used.

    Returns:
        pd.DataFrame: A DataFrame containing table details including operation patterns, file statistics,
                     optimization metrics, and health indicators.
    """
    if workspace:
        workspaceID = fabric.resolve_workspace_id(workspace)
    else:
        workspaceID = fabric.resolve_workspace_id()

    workspacename = fabric.resolve_workspace_name(workspaceID)

    def find_lhwh(workspaceID: str, lhwhid: str):
        lhwh = (fabric
                .list_items(workspace=workspaceID)
                .query("(Type == 'Warehouse' or Type == 'Lakehouse') and (Id == @lhwhid or `Display Name` == @lhwhid)")
                )
        lakehouseID = lhwh['Id'].max()
        lakehousename = lhwh['Display Name'].max()
        lakehousetype = lhwh['Type'].max()
        return lakehouseID, lakehousename, lakehousetype

    if not lakewarehouse:
        lakehouseID = fabric.get_lakehouse_id()
        lh_detail = find_lhwh(workspaceID, lhwhid=lakehouseID)
        lakehouseID = lh_detail[0]
        lhname = lh_detail[1]
        lhtype = lh_detail[2]
    else:
        try:
            lh_detail = find_lhwh(workspaceID, lhwhid=lakewarehouse)
            lakehouseID = lh_detail[0]
            lhname = lh_detail[1]
            lhtype = lh_detail[2]
        except Exception as e:
            print("Check workspace name/id, lakehouse/warehouse name/id")

    print(f">>>Scanning : Workspace: {workspacename}, {lhtype}: [{lhname},{lakehouseID}] ")
    filesystem_code = "abfss"
    onelake_account_name = "onelake"
    onelake_host = f"{onelake_account_name}.dfs.fabric.microsoft.com"
    base_path = f"{filesystem_code}://{workspaceID}@{onelake_host}/{lakehouseID}/"

    def get_filesystem():
        onelake_filesystem_class = fsspec.get_filesystem_class(filesystem_code)
        onelake_filesystem = onelake_filesystem_class(account_name=onelake_account_name, account_host=onelake_host)
        return onelake_filesystem

    def list_files_tables(workspaceID, lakehouseID):
        base_tables_path = base_path + "/Tables"
        found_paths = []
        try:
            direct_subfolders = notebookutils.fs.ls(base_tables_path)
        except Exception as e:
            print(f"Error listing directory {base_tables_path}: {e}")
            return pd.DataFrame(columns=['TableName', 'Path', 'SchemaTable'])

        for folder in direct_subfolders:
            delta_log_path = f"{folder.path}/_delta_log"
            try:
                if notebookutils.fs.ls(delta_log_path):
                    schema_table = folder.path.split('/Tables/')[1]
                    found_paths.append((folder.name, folder.path + '/', schema_table))
                    continue
            except Exception:
                pass

            try:
                subfolders = notebookutils.fs.ls(folder.path)
                for subfolder in subfolders:
                    delta_log_sub_path = f"{subfolder.path}/_delta_log"
                    try:
                        if notebookutils.fs.ls(delta_log_sub_path):
                            schema_table = subfolder.path.split('/Tables/')[1]
                            found_paths.append((schema_table, subfolder.name, subfolder.path + '/'))
                    except Exception:
                        pass
            except Exception as e:
                pass

        return pd.DataFrame(found_paths, columns=['Schema/Table', 'Table', 'Path'])

    def count_row_groups(valid_files, fs=get_filesystem()):
        return sum([pq.ParquetFile(f, filesystem=fs).num_row_groups for f in valid_files])

    def check_vorder(table_name_path):
        schema = ds.dataset(table_name_path, filesystem=get_filesystem()).schema.metadata
        if not schema:
            result = "Schema N/A"
        else:
            is_vorder = any(b'vorder' in key for key in schema.keys())
            result = str(schema[b'com.microsoft.parquet.vorder.enabled']) if is_vorder else "N/A"
        return result

    def table_details(workspaceID, lakehouseID, table_name, countrows=True):
        path = f'{base_path}/Tables/{table_name}'
        describe_query = "DESCRIBE DETAIL " + "'" + path + "'"
        history_query = "DESCRIBE HISTORY " + "'" + path + "'"

        # table details
        detail_df = spark.sql(describe_query).collect()[0]
        num_files = detail_df.numFiles
        size_in_bytes = detail_df.sizeInBytes
        size_in_mb = max(1, round(size_in_bytes / (1024 * 1024), 0))

        history_df = spark.sql(history_query)
        
        # Operation patterns
        total_operations = history_df.count()
        write_operations = history_df.filter(history_df.operation == 'WRITE').count()
        merge_operations = history_df.filter(history_df.operation == 'MERGE').count()
        delete_operations = history_df.filter(history_df.operation == 'DELETE').count()
        optimize_count = history_df.filter(history_df.operation == 'OPTIMIZE').count()
        vacuum_count = history_df.filter(history_df.operation == 'VACUUM END').count()
               
        current_timestamp = datetime.now()
        def get_days_since_operation(operation):
            op_history = history_df.filter(history_df.operation == operation).select('timestamp').collect()
            return (current_timestamp - op_history[0].timestamp).days if op_history else None
        
        days_since_write = get_days_since_operation('WRITE')
        days_since_optimize = get_days_since_operation('OPTIMIZE')
        days_since_vacuum = get_days_since_operation('VACUUM END')

        # Small writes
        small_writes = history_df.filter(
            (history_df.operation == 'WRITE') & 
            (history_df.operationMetrics.numFiles < 5)
        ).count()
        small_writes_percentage = (small_writes / max(1, write_operations)) * 100

        # Row count if enabled
        num_rows = spark.read.format("delta").load(path).count() if countrows else "Skipped"
        latest_files = spark.read.format("delta").load(path).inputFiles()
        file_names = [f.split(f"/Tables/{table_name}/")[-1] for f in latest_files]
        
        # Files
        onelake_filesystem = get_filesystem()
        valid_files = [f"{workspaceID}/{lakehouseID}/Tables/{table_name}/{file}" 
                      for file in file_names 
                      if onelake_filesystem.exists(f"{workspaceID}/{lakehouseID}/Tables/{table_name}/{file}")]

        # File size distribution
        file_sizes = [
            onelake_filesystem.size(f"{workspaceID}/{lakehouseID}/Tables/{table_name}/{file}") / (1024 * 1024)
            for file in file_names 
            if onelake_filesystem.exists(f"{workspaceID}/{lakehouseID}/Tables/{table_name}/{file}")
        ]
        
        median_file_size = int(np.median(file_sizes)) if file_sizes else 0
        small_files = sum(1 for size in file_sizes if size < 8)  # files smaller than 8MB, generally <8-32MB
        small_file_percentage = (small_files / len(file_sizes)) * 100 if file_sizes else 0

        # VORDER and rowgroup metrics
        is_vorder = set([check_vorder(file) for file in valid_files])
        
        with ThreadPoolExecutor() as executor:
            num_rowgroups = executor.submit(count_row_groups, valid_files).result()
            rowgroup_sizes = [pq.ParquetFile(f, filesystem=get_filesystem()).metadata.num_rows for f in valid_files]

        min_rowgroup_size = min(rowgroup_sizes) if rowgroup_sizes else 0
        max_rowgroup_size = max(rowgroup_sizes) if rowgroup_sizes else 0
        median_rowgroup_size = int(np.median(rowgroup_sizes)) if rowgroup_sizes else 0

        optimize_history = history_df.filter(history_df.operation == 'OPTIMIZE').select('timestamp').collect()
        last_optimize = optimize_history[0].timestamp if optimize_history else None
        vacuum_history = history_df.filter(history_df.operation == 'VACUUM END').select('timestamp').collect()
        last_vacuum = vacuum_history[0].timestamp if vacuum_history else None

        partitions = detail_df.partitionColumns or None

        return (
            workspaceID, lakehouseID, table_name, 
            num_files, num_rowgroups, num_rows, 
            int(size_in_mb), median_file_size, 
            is_vorder, 
            min_rowgroup_size, max_rowgroup_size, median_rowgroup_size,
            partitions, last_optimize, last_vacuum,
            write_operations, merge_operations, delete_operations,
            optimize_count, vacuum_count,
            days_since_write, days_since_optimize, days_since_vacuum,
            small_writes_percentage, small_file_percentage
        )

    table_list = list_files_tables(workspaceID, lakehouseID)['Schema/Table'].to_list()

    columns = [
        'Workspace ID', 'LW/WH ID', 'Table Name', 
        'Num_Files', 'Num_Rowgroups', 'Num_Rows', 
        'Delta_Size_MB', 'Median_File_Size_MB',
        'Is_VORDER',
        'Min_Rowgroup_Size', 'Max_Rowgroup_Size', 'Median_Rowgroup_Size',
        'Partition_Columns', 'Last OPTIMIZE Timestamp', 'Last VACUUM Timestamp',
        'Write_Operations', 'Merge_Operations', 'Delete_Operations',
        'Optimize_Count', 'Vacuum_Count',
        'Days_Since_Write', 'Days_Since_Optimize', 'Days_Since_Vacuum',
        'Small_Writes_Percentage', 'Small_Files_Percentage'
    ]

    details = [table_details(workspaceID, lakehouseID, t, countrows=countrows) for t in table_list]
    details_df = pd.DataFrame(details, columns=columns)

    # Split schema and table names
    details_df[['Schema Name', 'Table Name']] = details_df['Table Name'].apply(
        lambda x: pd.Series(x.split('/', 1) if '/' in x else ['', x])
    )

    columns_order = [
        'Workspace ID', 'LW/WH ID', 'Schema Name', 'Table Name',
        'Num_Files', 'Num_Rowgroups', 'Num_Rows',
        'Delta_Size_MB', 'Median_File_Size_MB',
        'Small_Files_Percentage',
        'Write_Operations', 'Merge_Operations', 'Delete_Operations',
        'Days_Since_Write',
        'Optimize_Count', 'Days_Since_Optimize',
        'Vacuum_Count', 'Days_Since_Vacuum',
        'Small_Writes_Percentage',
        'Is_VORDER',
        'Min_Rowgroup_Size', 'Max_Rowgroup_Size', 'Median_Rowgroup_Size',
        'Partition_Columns',
        'Last OPTIMIZE Timestamp', 'Last VACUUM Timestamp'
    ]
    
    details_df = details_df[columns_order]
    
    # is it a shortcut ? 
    if lhtype == "Lakehouse":
        shortcuts = labs.list_shortcuts(workspace=workspaceID, lakehouse=lhname)["Shortcut Name"].to_list()
        details_df["is_shortcut"] = details_df["Table Name"].apply(lambda table: table in shortcuts)
    else:
        details_df["is_shortcut"] = "N/A:Warehouse"

    return details_df.sort_values("Delta_Size_MB", ascending=False).reset_index(drop=True)
