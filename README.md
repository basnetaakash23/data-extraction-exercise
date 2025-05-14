# data-extraction-exercise

```
CREATE OR REPLACE PROCEDURE insert_order(
    p_invoice_no TEXT,
    p_stock_code TEXT,
    p_description TEXT,
    p_quantity INT,
    p_invoice_date TIMESTAMP,
    p_unit_price NUMERIC,
    p_customer_id BIGINT,
    p_country TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO orders (
        invoice_no, stock_code, description, quantity,
        invoice_date, unit_price, customer_id, country
    )
    VALUES (
        p_invoice_no, p_stock_code, p_description, p_quantity,
        p_invoice_date, p_unit_price, p_customer_id, p_country
    );
END;
$$;
```
### CREATE orders

```
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price DECIMAL(10,2),
    customer_id BIGINT,
    country VARCHAR(100)
);
```
### create sequence for usage with hibernate sequence id generation

```
DROP SEQUENCE IF EXISTS orders_v2_seq;

CREATE SEQUENCE orders_v2_seq INCREMENT BY 200;
```
