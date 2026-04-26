--C:/Users/tsvetkovds/Documents/.PROJECTS/SQL_PIPELINE/test.sql

CREATE OR REPLACE TABLE audit.test
ENGINE= MergeTree()
ORDER BY tuple()
AS (SELECT 1)