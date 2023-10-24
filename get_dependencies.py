def get_dependencies(dataset, workspace):
    

    """
    Returns dependecies of measures and calculated columns in specified dataset in a premium workspace. 
    Note that this works in Fabric notebook only but could easily be refactored for non-Premium and even for local models using APIs/XMLA.

    Note on how this works : 
    Use two DMVs, one to get dependencies and another to get folders. Join those two together to get the result df.
    To distinguish between a measure and a column, for columns the object names are Table[ColumnName]. 
    Measures and Calculated tables are just Object names.
    DAX Expression is not used anywhere. This is just for reference if the user neeeds it.

    Inputs:
        dataset : dataset id or name
        workspace: Premium workspace id or name

    Returns:
        pandas dataframe with columns: Object Name, Expression, Dependent on, Type, Folder
        Folder column has format : <TableName>\<Folder Name>\<Folder Name>

    """
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=UserWarning)

        dmv_dependency_query = """
            SELECT [OBJECT_TYPE], [TABLE], [OBJECT], [EXPRESSION], 
            [REFERENCED_OBJECT_TYPE], [REFERENCED_TABLE], [REFERENCED_OBJECT] 
            FROM $SYSTEM.DISCOVER_CALC_DEPENDENCY
            WHERE OBJECT_TYPE = 'MEASURE' OR OBJECT_TYPE = 'CALC_COLUMN'
        """

        dmv_folder_query = """
            SELECT [Name], [DisplayFolder] FROM $SYSTEM.TMSCHEMA_MEASURES
        """

        dependency_df = fabric.evaluate_dax(
            dataset=dataset,
            workspace=workspace,
            dax_string=dmv_dependency_query
        )

        folders_df = (
            fabric.evaluate_dax(
                dataset=dataset,
                workspace=workspace,
                dax_string=dmv_folder_query
            )
            .assign(OBJECT_TYPE="MEASURE") # dummy column for joining
        )

        merged_df = (
            dependency_df
            .merge(folders_df, left_on=['OBJECT_TYPE', 'OBJECT'], right_on=['OBJECT_TYPE', 'Name'], how='left')
            .assign(Folder=lambda x: x.TABLE + "\\" + x.DisplayFolder)
            .drop(['Name', 'DisplayFolder'], axis=1)
        )

        result_df = (
            merged_df
            .assign(
                **{
                    'Object Name': lambda x: np.where(
                        x.OBJECT_TYPE == "CALC_COLUMN",
                        x.TABLE + "[" + x.OBJECT + "]", #for calc column, Table[ColumnName]
                        x.OBJECT
                    ),
                    'Dependent on': lambda x: np.where(
                        x.REFERENCED_OBJECT_TYPE.str.contains("COLUMN"),
                        x.REFERENCED_TABLE + "[" + x.REFERENCED_OBJECT + "]",#for calc column, Table[ColumnName]
                        x.REFERENCED_OBJECT
                    )
                }
            )
            .query('REFERENCED_OBJECT_TYPE != "TABLE"')
            .drop(['OBJECT_TYPE', 'OBJECT', 'TABLE', 'REFERENCED_TABLE', 'REFERENCED_OBJECT'], axis=1)
            .rename(columns={'EXPRESSION': 'Expression', 'REFERENCED_OBJECT_TYPE': 'Type'})
            .reindex(columns=['Object Name', 'Expression', 'Dependent on', 'Type', 'Folder'])
            .reset_index(drop=True)
        )

    return result_df
