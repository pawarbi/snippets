### GET A LIST OF ALL THE POWER BI ITEMS IN THE TENANT
### https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items?tabs=HTTP

import pandas as pd
import requests
from typing import List, Dict, Any
import sempy.fabric as fabric 

client = fabric.FabricRestClient()

def get_powerbi_items(client, item_types: List[str] = None) -> pd.DataFrame:
    if item_types is None:
        item_types = ["Report", "Dashboard", "SemanticModel", "Datamart", "PaginatedReport", "App", "Dataflow"]
    
    all_items = []
    
    for item_type in item_types:
        try:
            response = client.get(f"/v1/admin/items?type={item_type}")
            
            if response.status_code != 200:
                print(f"Skipping {item_type}: API returned status code {response.status_code}")
                continue
                
            items_data = response.json()
            items = items_data.get("itemEntities", [])
            
            if not items:
                print(f"No items found of type: {item_type}")
                continue
                
            while "continuationToken" in items_data and items_data["continuationToken"]:
                token = items_data["continuationToken"]
                response = client.get(f"/v1/admin/items?type={item_type}&continuationToken={token}")
                if response.status_code == 200:
                    items_data = response.json()
                    items.extend(items_data.get("itemEntities", []))
                else:
                    print(f"Error during pagination for {item_type}: {response.status_code}")
                    break
                    
            for item in items:
                creator = item.get("creatorPrincipal", {})
                all_items.append({
                    "id": item.get("id"),
                    "type": item.get("type"),
                    "name": item.get("name"),
                    "state": item.get("state"),
                    "displayName": creator.get("displayName"),
                    "creatorId": creator.get("id"),
                    "workspaceId": item.get("workspaceId"),
                    "capacityId": item.get("capacityId"),
                    "lastUpdatedDate": item.get("lastUpdatedDate")
                })
            
            print(f"Successfully fetched {len(items)} items of type: {item_type}")
            
        except Exception as e:
            print(f"Error processing {item_type}: {str(e)}")
            continue
    
    if not all_items:
        print("No Power BI items found")
        return pd.DataFrame(columns=["id", "type", "name", "state", "displayName", 
                                    "creatorId", "workspaceId", "capacityId", "lastUpdatedDate"])
    
    df = pd.DataFrame(all_items)
    print(f"Created DataFrame with {len(df)} total items")
    return df


pbi_items = get_powerbi_items(client)
workspaces = fabric.list_workspaces()
