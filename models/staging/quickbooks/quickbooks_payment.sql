SELECT
    *
FROM {{ source('quickbooks','invoice')}}