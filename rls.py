def rls_member_list(dataset: str, workspace: str = None) -> pd.DataFrame:
    """
    Retrieve the list of RLS (Row-Level Security) role members for a given dataset and workspace.

    Parameters:
    dataset : str
        Dataset ID or dataset name.
    workspace : str, optional
        Workspace ID or workspace name. If None, the current workspace will be used.

    Returns:
    pd.DataFrame
        DataFrame containing the RLS role members. Returns empty DataFrame if no roles exist.
    """
    if workspace is None:
        workspace = fabric.get_workspace_id()

    try:
        tmsl_script = fabric.get_tmsl(dataset=dataset, workspace=workspace).replace("/n", "")
        model_data = json.loads(tmsl_script)["model"]
        
        # Check if 'roles' key exists in the model
        if "roles" not in model_data or not model_data["roles"]:
            return pd.DataFrame()  # Return empty DataFrame if no roles exist
            
        role_members = model_data["roles"]
        
        exploded = pd.json_normalize(role_members, sep="_").explode('members').reset_index(drop=True)
        df_members = pd.json_normalize(exploded['members'])
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    
    result_df = pd.concat([exploded.drop(columns=['members']), df_members], axis=1, 
                         ignore_index=False).reset_index(drop=True)
    
    # Drop modifiedTime column if it exists
    if "modifiedTime" in result_df.columns:
        result_df = result_df.drop("modifiedTime", axis=1)
    
    return result_df  # Return the result DataFrame instead of role_members

# Example usage
dataset = 'RLSTest_Empty'
workspace = None
r = rls_member_list(dataset, workspace)
display(r)
