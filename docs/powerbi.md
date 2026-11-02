# Power BI report

Power BI Desktop is Windows-only. On a Mac, use a UIUC computer lab machine, a Windows VM, or
Parallels. Your illinois.edu account normally includes Power BI; you need it only to *publish*,
not to build the report in Desktop.

## Connect

1. **Get data → Azure SQL database**
   * Server: `sql-<name>.database.windows.net` (from `Deploy-All.ps1` output)
   * Database: `tri_gold`
   * Data connectivity mode: **Import** (the data refreshes monthly; Import is fast and
     lets the free serverless database auto-pause instead of staying awake for DirectQuery)
2. Auth: **Microsoft account** → sign in with the account you deployed with (it's the Entra
   admin on the SQL server). Deploy-All added your public IP to the SQL firewall; if your IP
   changed, re-run `Deploy-All.ps1` or add it in Portal → SQL server → Networking.
3. The first connection can take ~1 minute while the paused database resumes.
4. Load these tables/views:
   `gold_facility_releases`, `gold_chemical_state_releases`, `gold_parent_company_releases`,
   `gold_data_quality`, `vw_facility_map`, `vw_state_summary`, `vw_dow_facilities`

## Measures (DAX)

```DAX
Total Releases (M lb) = DIVIDE(SUM(gold_facility_releases[total_releases_lb]), 1000000)
Air Share = DIVIDE(SUM(gold_facility_releases[air_releases_lb]), SUM(gold_facility_releases[total_releases_lb]))
Carcinogen Share = DIVIDE(SUM(gold_facility_releases[carcinogen_releases_lb]), SUM(gold_facility_releases[total_releases_lb]))
Reporting Facilities = DISTINCTCOUNT(gold_facility_releases[trifd])
PFAS Facilities = CALCULATE([Reporting Facilities], gold_facility_releases[reports_pfas] = TRUE())
```

## Pages

| Page | Visuals |
|---|---|
| **Overview** | Cards: Total Releases (M lb), Reporting Facilities, Carcinogen Share, PFAS Facilities · Bar: top 10 parent companies (`vw_parent_company_top25`) · Slicer: reporting_year |
| **Map** | Map/Azure Map from `vw_facility_map` (latitude, longitude), bubble size = total_releases_lb, tooltip = facility, parent, sector · Slicers: state, industry_sector |
| **Chemicals** | Matrix: state × chemical from `gold_chemical_state_releases` · Bar: top chemicals by release · Filter toggle: is_carcinogen, is_pfas |
| **Dow** | Table + map from `vw_dow_facilities`: Dow's own reporting sites, releases by medium (air/water/on-site/off-site) |
| **Data quality** | Table/cards from `gold_data_quality`: registry_match_rate, rows_converted_from_grams, rows_missing_coordinates, rows_missing_parent |

The **Dow** page is the one to show in the interview: it uses their own public data.
Check the label first (`SELECT DISTINCT parent_company_label FROM gold_facility_releases WHERE parent_company_label LIKE '%DOW%'`)
and adjust `vw_dow_facilities` if EPA spells it differently.
