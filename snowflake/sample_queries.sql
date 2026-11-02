-- Queries to run in Snowsight after a pipeline run with the Snowflake load enabled.
USE ROLE TRI_LOADER;
USE WAREHOUSE TRI_WH;
USE SCHEMA TRI_DB.GOLD;

-- Top 10 parent companies by total releases in the latest year
SELECT reporting_year, rank_in_year, parent_company_label, facilities, states,
       ROUND(total_releases_lb / 1e6, 2) AS total_releases_million_lb
FROM PARENT_COMPANY_RELEASES
WHERE reporting_year = (SELECT MAX(reporting_year) FROM PARENT_COMPANY_RELEASES)
ORDER BY rank_in_year
LIMIT 10;

-- Carcinogen share of releases by state
SELECT reporting_year, state,
       SUM(total_releases_lb) AS total_lb,
       SUM(IFF(is_carcinogen, total_releases_lb, 0)) AS carcinogen_lb,
       ROUND(carcinogen_lb / NULLIF(total_lb, 0), 4) AS carcinogen_share
FROM CHEMICAL_STATE_RELEASES
GROUP BY reporting_year, state
ORDER BY carcinogen_lb DESC
LIMIT 15;

-- Pipeline data-quality summary
SELECT * FROM DATA_QUALITY ORDER BY reporting_year DESC;
