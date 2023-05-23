
select 
    CONTENT:type::string as type,
    CONTENT:name::string as name,
    CONTENT:cash::string as cash,
    CONTENT:invests::string as invests,
    CONTENT:lastFixedPrice::string as lastFixedPrice,
    CONTENT:obName::string as ISIN,
    CONTENT:timestamp::string as timestamp
  from TRADE_BRONZE, lateral flatten( input => CONTENT) where type='Agent'
