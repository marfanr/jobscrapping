{{ config(
    materialized = 'view',
    view_security ='invoker'
) }}

SELECT job_name, url FROM iceberg.bronze.jobs