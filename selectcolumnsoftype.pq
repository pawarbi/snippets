//https://pawarbi.github.io/blog/powerquery/m/powerbi/2021/05/04/selecting-columns-columntype-powerbi.html
//Author: Sandeep Pawar
let
  SourceTable = Table.FromRecords(
    {
      [
        CustomerID = 1, 
        JerseyNo   = 45, 
        Name       = "Rohit", 
        LastName   = "Sharma", 
        Phone      = "123-4567", 
        Date1      = "1/1/2020", 
        Date2      = "1/1/2020"
      ], 
      [
        CustomerID = 2, 
        JerseyNo   = 10, 
        Name       = "Sachin", 
        LastName   = "Tendulkar", 
        Phone      = "123-4567", 
        Date1      = "1/1/2020", 
        Date2      = "1/1/2020"
      ]
    }, 
    type table [
      CustomerID = Number.Type, 
      JerseyNo = Number.Type, 
      Name = Text.Type, 
      LastName = Text.Type, 
      Date1 = Date.Type, 
      Date2 = Date.Type
    ]
  ), 
  SourceTable2 = Table.TransformColumnTypes(
    SourceTable, 
    {{"Date1", type date}, {"Date2", type date}}
  ), 
  //Begin Transformation

  
   //Unpivot Columns, only grab the first two rows of the table to save time 
  smalltable1 = Table.FirstN(SourceTable2,5),

  UnpivotedColumns = Table.UnpivotOtherColumns(smalltable1, {}, "ColumnName", "Value"), 
  //Remove Duplicates from Column Names so get only column names
  RemoveDuplicates = Table.Distinct(UnpivotedColumns, {"ColumnName"}), 
  //Add Custom Column to detect the column type
  DetectColumnType = Table.AddColumn(
    RemoveDuplicates, 
    "ColumnType", 
    each 
      if Type.Is(Value.Type([Value]), Date.Type) then
        "Date"
      else if Type.Is(Value.Type([Value]), Number.Type) then
        "Number"
      else if Type.Is(Value.Type([Value]), Text.Type) then
        "Text"
      else
        "Other"
  ), 
  SelectedColumns = Table.SelectRows(DetectColumnType, each [ColumnType] = _fnSelectColumnType), 
  FinalColumns = SelectedColumns[ColumnName], 
  //If "All" is selected return original Column, otherwise return selected columns
  FinalTable = 
    if _fnSelectColumnType = "All" then
      SourceTable2
    else
      Table.SelectColumns(SourceTable2, FinalColumns)
in
  FinalTable
