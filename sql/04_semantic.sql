-- 04_semantic.sql
-- Run separately after the Bronze/Silver/Gold Lakeflow pipeline completes.

CREATE OR REPLACE VIEW rearc.gold.v_us_population_stats_2013_2018 AS
SELECT MIN(year) AS start_year, MAX(year) AS end_year, COUNT(*) AS number_of_years,
       AVG(population) AS mean_population, STDDEV_SAMP(population) AS stddev_population
FROM rearc.gold.us_population_yearly
WHERE year BETWEEN 2013 AND 2018;

CREATE OR REPLACE VIEW rearc.gold.v_bls_series_best_year AS
SELECT series_id, series_label, best_year, best_year_value, quarter_count
FROM rearc.gold.bls_series_best_year;

CREATE OR REPLACE VIEW rearc.gold.v_prs30006032_q01_population AS
SELECT series_id, year, period, value, population
FROM rearc.gold.bls_population_analysis
WHERE series_id = 'PRS30006032' AND period = 'Q01';
