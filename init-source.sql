CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO orders (customer_name, amount, status) VALUES
    ('Alice', 120.50, 'pending'),
    ('Bob', 75.00, 'pending'),
    ('Charlie', 200.00, 'pending');
