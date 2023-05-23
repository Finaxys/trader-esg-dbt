select 
    CONTENT:type::string as type,
    CONTENT:timestamp::string as timestamp,
    CONTENT:sender::string as sender,
    CONTENT:orderType::string as orderType,
    CONTENT:obName::string as ISIN,
    CONTENT:price::int as price,
    CONTENT:quantity::int as quantity,
    CONTENT:extId::int as extId,
    CONTENT:validity::int as validity
  from TRADE_BRONZE, lateral flatten( input => CONTENT) where type='Order'