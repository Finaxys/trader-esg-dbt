WITH OB_HOLDING AS (
    SELECT
      a.NAME as agentname, 
      LEFT(a.OBNAME, 12) as orderbook, 
      a.INVESTS * a.LASTFIXESPRICE as ob_holding
    FROM {{ref('BRONZE_TABLE_AGENT')}} a
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agentname, orderbook ORDER BY  a.TIMESTAMP desc) = 1)
, HOLDING AS (
    SELECT AGENTNAME, 
        sum(ob_holding) as holding
    FROM OB_HOLDING GROUP BY AGENTNAME
), HOLDING_RATIO AS (
    SELECT 
        obh.AGENTNAME,
        obh.orderbook, 
        DIV0(obh.ob_holding,h.holding) as holding_ratio, 
        h.holding as holding, 
        obh.ob_holding 
    FROM OB_HOLDING obh, HOLDING H
    WHERE obh.agentname = h.agentname
), OB_ESG_SCORE AS (
    SELECT AGENTNAME, ORDERBOOK,
    holding_ratio,
    holding, 
    ob_holding,
    (holding_ratio * esg.esg) as esg_ratio,
     (holding_ratio * esg.e) as e_ratio,
     (holding_ratio * esg.s) as s_ratio,
      (holding_ratio * esg.g) as g_ratio
    FROM HOLDING_RATIO hr
    LEFT OUTER JOIN {{ref('BRONZE_TABLE_ESG')}} esg on orderbook = esg.isin
), ESG_RATING AS (
    SELECT 
        AGENTNAME,
        max(holding) as holding,
        sum(esg_ratio) as esg,
        sum(e_ratio) as e,
        sum(s_ratio) as s,
        sum(g_ratio) as g
    FROM OB_ESG_SCORE
    GROUP BY AGENTNAME
)
SELECT * FROM ESG_RATING ORDER BY holding desc