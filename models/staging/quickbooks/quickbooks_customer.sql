SELECT
    *
FROM {{ source('quickbooks','customer')}}