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

CREATE SEQUENCE orders_v2_seq INCREMENT BY 50;
```

## Application overview

This repository contains a small Maven-based Spring Boot application that
imports CSV data into PostgreSQL using two different approaches.

- **DataExtractionApplication** – bootstraps Spring Boot.
- **controller/DataController** – exposes `/process-data/jdbc` and
  `/process-data/hibernate` endpoints to trigger the import.
- **service/DataProcessor** – calls the `insert_order` stored procedure in
  batches and logs malformed lines to `error_log.txt`.
- **service/DataProcessorHibernate** – imports the same CSV data with JPA,
  flushing the `EntityManager` every 50 records.
- **domain/Orders** and **repository/OrderRepository** – JPA entity and
  repository mapped to the `orders_v2` table.

Configuration settings live in `src/main/resources/application.properties` and a
sample dataset `data.csv` is provided in the same directory. Running the
application requires the table, sequence, and stored procedure definitions shown
above.

A single test class (`DataExtractionApplicationTests`) currently verifies that
the Spring context loads. Expanding tests and error handling would be useful
next steps for future development.
