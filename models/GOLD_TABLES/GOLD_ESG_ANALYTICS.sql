WITH ESG_RATING AS (
    SELECT 
        agentname,
        max(holding) as holding,
        sum(esg_ratio) as esg,
        sum(e_ratio) as e,
        sum(s_ratio) as s,
        sum(g_ratio) as g
    FROM {{ref('SILVER_ESG_ANALYTICS')}}
    GROUP BY agentname
)
SELECT * FROM ESG_RATING ORDER BY holding desc