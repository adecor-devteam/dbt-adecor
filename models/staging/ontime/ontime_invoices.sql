SELECT
    *
FROM {{ source('ontime','invoices')}}