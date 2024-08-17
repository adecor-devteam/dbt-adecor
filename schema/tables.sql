CREATE TABLE customer_details (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY NOT NULL,
    ontime_customerid CHARACTER VARYING(256) UNIQUE,
    qb_customerid CHARACTER VARYING(256) UNIQUE,
    cardholder_first_name CHARACTER VARYING(256),
    cardholder_last_name CHARACTER VARYING(256),
    stripe_customer_id CHARACTER VARYING(256),
    convenience_fee NUMERIC DEFAULT 2.99,
    gst NUMERIC DEFAULT 5.0,
    discount NUMERIC DEFAULT 0,
    reward_air_miles BOOLEAN DEFAULT FALSE,
    air_miles_start_date TIMESTAMP,
    air_miles_number INTEGER,
    total_rewards INTEGER,
    edi_file BOOLEAN DEFAULT FALSE,
    edi_account_name CHARACTER VARYING(256),
    edi_file_name CHARACTER VARYING(256),
    edi_date_format CHARACTER VARYING(256),
    upload_csv_template BYTEA,
    pdf_invoices BOOLEAN DEFAULT TRUE,
    email_invoices BOOLEAN DEFAULT TRUE,
    qb_invoices BOOLEAN DEFAULT TRUE,
    qb_update_customer BOOLEAN DEFAULT FALSE
);

CREATE TABLE customer_details_defaults (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY NOT NULL,
    ontime_customerid CHARACTER VARYING(256) UNIQUE,
    qb_customerid CHARACTER VARYING(256) UNIQUE,
    cardholder_first_name CHARACTER VARYING(256),
    cardholder_last_name CHARACTER VARYING(256),
    stripe_customer_id CHARACTER VARYING(256),
    convenience_fee NUMERIC DEFAULT 2.99,
    gst NUMERIC DEFAULT 5.0,
    discount NUMERIC DEFAULT 0,
    reward_air_miles BOOLEAN DEFAULT FALSE,
    air_miles_start_date TIMESTAMP,
    air_miles_number INTEGER,
    total_rewards INTEGER,
    edi_file BOOLEAN DEFAULT FALSE,
    edi_account_name CHARACTER VARYING(256),
    edi_file_name CHARACTER VARYING(256),
    edi_date_format CHARACTER VARYING(256),
    upload_csv_template BYTEA,
    pdf_invoices BOOLEAN DEFAULT TRUE,
    email_invoices BOOLEAN DEFAULT TRUE,
    qb_invoices BOOLEAN DEFAULT TRUE,
    qb_update_customer BOOLEAN DEFAULT FALSE
);


insert into customer_details_defaults ( ontime_customerid ) values ( 'default'); 


#ALTER TABLE customer_details ADD COLUMN qb_update_customer BOOLEAN DEFAULT FALSE;

CREATE TABLE quickbooks_tokens (
    user_id VARCHAR(255) PRIMARY KEY,
    refresh_token BYTEA NOT NULL
);

GRANT SELECT ON quickbooks_tokens TO invoicegenerator;
GRANT UPDATE ON quickbooks_tokens TO invoicegenerator;
GRANT INSERT ON quickbooks_tokens TO invoicegenerator;



GRANT SELECT ON customer_details TO invoicegenerator;
GRANT UPDATE ON customer_details TO invoicegenerator;
GRANT INSERT ON customer_details TO invoicegenerator;
GRANT SELECT ON customer_details_defaults TO invoicegenerator;
GRANT UPDATE ON customer_details_defaults TO invoicegenerator;
GRANT INSERT ON customer_details_defaults TO invoicegenerator;


GRANT SELECT ON customer_details_defaults TO invoicegenerator;


ALTER TABLE invoice_tracking ADD COLUMN qb_invoicenumber CHARACTER VARYING(256);
ALTER TABLE invoice_tracking ADD COLUMN qb_invoice_status BOOLEAN;
ALTER TABLE invoice_tracking ADD COLUMN qb_invoice_created_at TIMESTAMP;
ALTER TABLE invoice_tracking ADD COLUMN qb_invoice_error TEXT;
