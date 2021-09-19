'''
Created By Sandeep Pawar 
Date: 9/19/2021
Scrpt to connect to Power BI desktop dataset.
Usage:

  1.copy server name and db_name from DAX Studio or SSMS
  2.In the code below, wherever you see table_name, replace it with the name of your table from Power BI desktop. e.g. SalesFact or CustomerDim etc. 
  3.Step #2 is optional, it helps clean up the column names from the resulting table. One limitation right now is that table name cannot have a space. 
    e.g. 'table name' will not work. Before you import change the table name in Power BI to remove spaces temporarily
  4. If you don't want to change table name, skip teh column renaming step below and just setup connection (conn) and import df
'''

server = 'localhost:xxxxxx'
db_name = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

dax = '''
    //Write your DAX Query here
    EVALUATE
    table_name 
    
    '''

def get_powerbi(server:str, db:str, dax, table_name:str):
    
    
    conn = ssas.set_conn_string(
    server=server,
    db_name=db_name,
    username='',
    password=''
    )
     
    
    df =     (ssas
                   .get_DAX(
                   connection_string=conn, 
                   dax_string=dax)
               
              )
    
    df.columns = [x.replace(table_name,'').replace('[','').replace(']','') for x in df.columns]

    return df


df = get_powerbi(server, db_name, dax, 'table_name')
