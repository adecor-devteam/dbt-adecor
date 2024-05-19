SELECT
    *
FROM {{ source('stripe','customer')}}