import snowflake.snowpark as snowpark



def model(dbt, session): 
    
    ESG_df = session.table('TRADE_DB.PUBLIC.BRONZE_TABLE_ESG')
    
    PRICE_df = session.table('TRADE_DB.PUBLIC.BRONZE_TABLE_PRICE')
   
    #join the two dfs 'ESG_df' and 'PRICE_df'
    dataframe = ESG_df.join(PRICE_df, ESG_df['ISIN']==PRICE_df['OBNAME'])


    return dataframe