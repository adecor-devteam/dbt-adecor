SELECT
    *
FROM {{ source('ontime','destinations')}}