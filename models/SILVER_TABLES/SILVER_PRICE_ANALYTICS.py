

def model(dbt, session): 
    
<<<<<<< HEAD:models/SILVER_TABLES/SILVER_PRICE_ANALYTICS.py
    CLASSIFICATION_df = dbt.ref("BRONZE_TABLE_CLASSIFICATION")
    
    PRICE_df = dbt.ref("BRONZE_TABLE_PRICE")
=======
    ESG_df = session.table('BRONZE_TABLE_ESG')
    
    PRICE_df = session.table('BRONZE_TABLE_PRICE')
>>>>>>> a3e9e440dc8c6ccf7ba59d263199373cf3a4c616:models/PYTHON_MODEL.py
       #Lambda function to cumpute volume
    f = lambda x : x['PRICE'] * x['QUANTITY']
    PRICE_df = PRICE_df.to_pandas()
    
    #use apply fucntion to parse the df
    PRICE_df['VOLUME'] = PRICE_df.apply(f, axis=1)
    #reorder the columns in the df
    vol = PRICE_df.pop('VOLUME')
    PRICE_df.insert(4, 'VOLUME', vol)
    #convert the pandas df to snowpark df
    PRICE_df = session.create_dataframe(PRICE_df)
    #join the two dfs 'ESG_df' and 'PRICE_df'
    dataframe = CLASSIFICATION_df.join(PRICE_df, CLASSIFICATION_df['ISIN']==PRICE_df['OBNAME'])

    return dataframe