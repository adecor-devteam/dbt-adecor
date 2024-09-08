SELECT
    *
FROM {{ source('quickbooks','invoice_linked_txn')}}