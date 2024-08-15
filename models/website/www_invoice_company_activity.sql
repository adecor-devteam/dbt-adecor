-- Invoice Number - (ontime_invoices)
-- Payment Status - (quickbooks_invoice) If Balance = 0 then Paid, Otherwise Unpaid
-- Emails Sent - (invoice_tracking)
-- Invoice Status - (quickbooks_invoice) If Balance > 0 then Calculate days from Created Date (i.e. 5 days)
-- Invoice Amount - (invoice_tracking)
-- Rewards - (invoice_tracking) If Invoice is Paid and Rewards in (customer_details) = True Calculates total rewards from Subtotal * 0.025 and then rounds down to nearest ($40). In short the rewards is awarded 1 mile for every $40.
-- Discount - (customer_details)
-- GST - (customer_details)
-- Convenience Fee - (customer_details)
-- Total Owing - (quickbooks_invoice)
-- Last Updated - This is the last time the invoice was processed so I believe you can use the qb_invoice_created_at column


