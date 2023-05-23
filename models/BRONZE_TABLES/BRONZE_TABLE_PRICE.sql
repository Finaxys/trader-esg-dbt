
select 
    CONTENT:type::string as type,
    CONTENT:timestamp::int as timestamp,
    CONTENT:quantity::int as quantity,
    CONTENT:price::int as price,
    CONTENT:extId2::string as extId2,
    CONTENT:extId1::string as extId1,
    CONTENT:obName::string as ISIN,
    CONTENT:direction::string as direction,
    CONTENT:bestBidPrice::int as bestBidPrice,
    CONTENT:bestAskPrice::int as bestAskPrice
  from TRADE_BRONZE, lateral flatten( input => CONTENT) where type='Price'
