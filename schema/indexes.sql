-- action_ontime_dbo.invoices

CREATE INDEX idx_invoicenumber ON action_ontime_dbo.invoices (invoicenumber);
CREATE INDEX idx_invoicenumber_text ON action_ontime_dbo.invoices ((invoicenumber::TEXT));
CREATE INDEX idx_invoices_customerid ON action_ontime_dbo.invoices (customerid);
CREATE INDEX idx_invoices_customerid_hash ON action_ontime_dbo.invoices USING hash(customerid);
CREATE INDEX idx_invoices_creationdate ON action_ontime_dbo.invoices (creationdate);

CREATE INDEX idx_invoices_gin_orders ON action_ontime_dbo.invoices USING GIN (string_to_array(upper(orders), ','));




-- action_ontime_dbo.tracking
CREATE INDEX idx_orderid ON action_ontime_dbo.tracking (orderid);
CREATE INDEX idx_dto ON action_ontime_dbo.tracking (dto);
CREATE INDEX idx_dfrom ON action_ontime_dbo.tracking (dfrom);
CREATE INDEX idx_droppedoffbydriverid ON action_ontime_dbo.tracking (droppedoffbydriverid);
CREATE INDEX idx_priceset ON action_ontime_dbo.tracking (priceset);
CREATE INDEX idx_vehiclerequired ON action_ontime_dbo.tracking (vehiclerequired);
CREATE INDEX idx_vehiclerequired ON action_ontime_dbo.tracking (vehiclerequired);


-- action_ontime_dbo.tracking
CREATE INDEX idx_creationdate ON action_ontime_dbo.tracking (creationdate);
CREATE INDEX idx_whendroppedoff_mst ON action_ontime_dbo.tracking ((whendroppedoff AT TIME ZONE 'UTC' AT TIME ZONE 'MST'));
CREATE INDEX idx_driver_name ON action_ontime_dbo.users ((firstname || ' ' || lastname))


-- trackingmodifiers
CREATE INDEX idx_trackingmodifiers_name ON action_ontime_dbo.trackingmodifiers (name);
CREATE INDEX idx_trackingmodifiers_trackingid ON action_ontime_dbo.trackingmodifiers (trackingid);


-- arms_email_activity_feed

CREATE INDEX idx_batch_message_id ON arms_email_activity_feed (batch_message_id);
CREATE INDEX idx_arms_email_activity_feed_status ON arms_email_activity_feed (status);
CREATE INDEX idx_arms_email_activity_feed_invoice_number ON arms_email_activity_feed (invoice_number);

-- invoice_email_tracking

CREATE INDEX idx_invoice_email_tracking_invoiceid ON invoice_email_tracking (invoiceid);
CREATE INDEX idx_invoice_email_tracking_pdf_sendgrid_messageid ON invoice_email_tracking (pdf_sendgrid_messageid);


-- customer_details

CREATE INDEX idx_customer_details_ontime_customerid ON customer_details (ontime_customerid);
CREATE INDEX idx_customer_details_ontime_customerid_hash ON customer_details USING hash(ontime_customerid);



-- invoice_tracking
CREATE INDEX idx_invoice_tracking ON invoice_tracking (qb_invoice_created_at);

-- action_quickbooks.invoice

CREATE INDEX idx_invoice_doc_number on action_quickbooks.invoice (doc_number);
CREATE INDEX idx_invoice_balance on action_quickbooks.invoice (balance);