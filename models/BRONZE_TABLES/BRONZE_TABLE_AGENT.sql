SELECT
    $1:timestamp::varchar::TIMESTAMP TIMESTAMP,
    $1:name::STRING NAME,
    $1:cash::NUMBER CASH,
    $1:obName::STRING OBNAME,
    $1:invests::NUMBER INVESTS,
    $1:lastFixedPrice::NUMBER LASTFIXESPRICE
FROM
    {{ source('TRADE_DB', 'RAW') }}
WHERE
    $1:type = 'Agent'