import snowflake.snowpark as snowpark



def model(dbt, session): 
    
    ESG_df = session.table('TRADE_DB.PUBLIC.BRONZE_TABLE_ESG')
    PRICE_df = session.table('TRADE_DB.PUBLIC.BRONZE_TABLE_PRICE')
    dataframe = ESG_df.join(PRICE_df, "ISIN")


    return dataframe