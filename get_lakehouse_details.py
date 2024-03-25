import re
import concurrent.futures
import pandas as pd
impory sempy.fabric as fabric

def get_lakehouse_details_concurrently(dataset):
    def extract_details(s):
        match = re.search(r'Sql\.Database\("(.*?)", "(.*?)"\)', s)
        if match:
            sqlendpoint, lakewarehousename = match.groups()
            return sqlendpoint, lakewarehousename
        else:
            return None, None

    sqlendpoint, lakewarehouse = extract_details(fabric.list_expressions(dataset).Expression.max())
    print("endpoint: ", sqlendpoint, " lh/wh:", lakewarehouse)

    def get_items_for_workspace(ws):
        return fabric.list_items(workspace=ws)

    def get_items_concurrently(workspaces):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(get_items_for_workspace, ws) for ws in workspaces]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        return pd.concat(results, ignore_index=True)

    dedicated_workspaces = fabric.list_workspaces().query('`Is On Dedicated Capacity` == True').Id
    df = get_items_concurrently(dedicated_workspaces)
    lakehouses = df.query('`Type`=="Lakehouse" & `Display Name`==@lakewarehouse')

    client = fabric.FabricRestClient()

    def get_lakehouse_details(workspaceId, lakehouseId):
        response = client.get(f"v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}")
        return pd.json_normalize(response.json())

    def get_lakehouse_details_concurrently(lakehouses):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(get_lakehouse_details, row['Workspace Id'], row['Id']) for i, row in lakehouses.iterrows()]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        return pd.concat(results, ignore_index=True)

    return get_lakehouse_details_concurrently(lakehouses)
