SELECT
    *
FROM {{ source('stripe','charge')}}