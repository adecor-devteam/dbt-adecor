SELECT 
*,
unnest(string_to_array(upper(orders), ',')) as tracking_id 
FROM {{ source('ontime','invoices')}}