SELECT
    *
FROM {{ source('quickbooks','payment_method')}}