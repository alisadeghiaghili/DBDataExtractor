import pandas as pd
import sqlalchemy as sa

db_connection_string = 'mssql+pyodbc://172.16.1.119/Future_DM?driver=SQL+Server+Native+Client+11.0'
engine = sa.create_engine(db_connection_string)


schemas = ['Auction_Dim', 'Auction_Fact']

def extract_tables(db_connection_string, schemas, engine):
    # Create SQLAlchemy engine
    engine = engine
    
    
    resultDict = {}
    
    for schema in schemas:
        
        # Create MetaData object
        metadata = sa.MetaData()
        
        # Reflect database schema into MetaData    
        metadata.reflect(bind=engine, schema=schema)
    
        # Get all tables from MetaData
        tables = metadata.tables.values()
        
        resultDict[schema] = [table.name for table in tables]
        
    # Return dictionary of schema with table names
    return resultDict

extracted_tables = extract_tables(db_connection_string, schemas, engine)
print(extracted_tables)


for schema in extracted_tables.keys():
    for table in extracted_tables[schema]:
        try:
            pd.read_sql('select * from Auction_DM.' +  schema + '.' + table, engine).head(100).to_excel('D:\\ExtractedData\\' + schema + '_' + table + '.xlsx')
        except:
            pass
