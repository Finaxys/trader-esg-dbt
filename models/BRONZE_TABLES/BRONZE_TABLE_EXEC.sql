select 
    CONTENT:type::string as type,
    CONTENT:validity::string as validity,
    CONTENT:timestamp::string as timestamp,
    CONTENT:sender::string as sender,
    CONTENT:quantity::string as quantity,
    CONTENT:price::string as price,
    CONTENT:orderType::string as orderType,
    CONTENT:obName::string as ISIN,
    CONTENT:extId::string as extId
  from TRADE_BRONZE, lateral flatten( input => CONTENT) where type='Exec'