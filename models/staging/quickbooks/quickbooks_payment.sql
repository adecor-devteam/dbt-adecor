SELECT
    *
FROM {{ source('quickbooks','payment')}}