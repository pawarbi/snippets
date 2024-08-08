import sempy.fabric as fabric 
import pandas as pd 
client = fabric.FabricRestClient()

## https://fabric.guru/use-ai-skills-as-cell-magic-in-fabric-notebook

def ask_aiskill(workspace, aiskill, question, instructions):
    payload = {"userQuestion": question}        
        
    url = f"v1/workspaces/{workspace}/aiskills/{aiskill}/query/deployment"
    response = client.post(url, json = payload)
    result = response.json()
    result_header = result['ResultHeaders']
    result_rows = result['ResultRows']
    df = pd.DataFrame(result_rows, columns = result_header)


    return df
