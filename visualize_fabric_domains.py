import sempy.fabric as fabric
import pandas as pd
from graphviz import Digraph
from IPython.display import display

client = fabric.FabricRestClient()

import pandas as pd

def get_domains_and_workspaces_with_names(client):
    # get all domains
    domains = pd.DataFrame(client.get(f"v1/admin/domains").json()['domains'])
        
    domain_data = []
    domain_name_map = domains.set_index('id')['displayName'].to_dict()

    # get domain details
    for _, row in domains.iterrows():
        domain_id = row['id']
        display_name = row['displayName']
        parent_domain_id = row['parentDomainId']
        parent_domain_name = domain_name_map.get(parent_domain_id, 'None')
       
        try:
            workspaces = get_domain_ws(domain_id)['displayName'].to_list()
        except:
            workspaces = []

        # result df
        domain_data.append({
            'Domain ID': domain_id,
            'Domain Name': display_name,
            'Parent Domain Name': parent_domain_name,
            'Workspaces': ', '.join(workspaces) if workspaces else 'No Workspaces'
        })
   
    return pd.DataFrame(domain_data)

domains_with_workspaces = get_domains_and_workspaces_with_names(client)
domains_with_workspaces



def visualize_domains(domains_df, orientation='LR'):
    """
    orientation : "LR" default (Left to Right) , "TB": top to bottom

    """
    dot = Digraph(comment="Domain Hierarchy with Workspaces", graph_attr={'rankdir': orientation})

    # nodes
    for _, row in domains_df.iterrows():
        domain_name = row['Domain Name']
        parent_domain_name = row['Parent Domain Name']
        workspaces = row['Workspaces']
        dot.node(domain_name, domain_name)
        if parent_domain_name != 'None':
            dot.edge(parent_domain_name, domain_name)

        if workspaces != 'No Workspaces':
            for workspace in workspaces.split(', '):
                dot.node(workspace, workspace, shape='box')
                dot.edge(domain_name, workspace)
   
    display(dot)

visualize_domains(domains_with_workspaces, orientation='LR')


