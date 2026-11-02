-- Reporting views for Power BI. Applied by 04_load_azure_sql after every load.
-- Batches are separated by a line containing only GO.

CREATE OR ALTER VIEW dbo.vw_parent_company_top25 AS
SELECT reporting_year,
       parent_company_label,
       facilities,
       states,
       chemicals,
       total_releases_lb,
       carcinogen_releases_lb,
       rank_in_year
FROM dbo.gold_parent_company_releases
WHERE rank_in_year <= 25;
GO

CREATE OR ALTER VIEW dbo.vw_facility_map AS
SELECT reporting_year,
       trifd,
       facility_name,
       city,
       state,
       latitude,
       longitude,
       industry_sector,
       parent_company_label,
       total_releases_lb,
       air_releases_lb,
       water_releases_lb,
       carcinogen_releases_lb,
       reports_pfas
FROM dbo.gold_facility_releases
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL;
GO

CREATE OR ALTER VIEW dbo.vw_state_summary AS
SELECT reporting_year,
       state,
       COUNT(*)                    AS facilities,
       SUM(total_releases_lb)      AS total_releases_lb,
       SUM(air_releases_lb)        AS air_releases_lb,
       SUM(water_releases_lb)      AS water_releases_lb,
       SUM(carcinogen_releases_lb) AS carcinogen_releases_lb,
       SUM(CASE WHEN reports_pfas = 1 THEN 1 ELSE 0 END) AS facilities_reporting_pfas
FROM dbo.gold_facility_releases
GROUP BY reporting_year, state;
GO

-- Dow's own reporting facilities. Check the exact parent label in your data
-- (SELECT DISTINCT parent_company_label ... LIKE '%DOW%') and adjust if needed.
CREATE OR ALTER VIEW dbo.vw_dow_facilities AS
SELECT *
FROM dbo.gold_facility_releases
WHERE UPPER(parent_company_label) LIKE 'DOW INC%'
   OR UPPER(parent_company_label) LIKE '%DOW CHEMICAL%';
GO
