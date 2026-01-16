CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  user_id INT,
  amount DECIMAL,
  status VARCHAR(20),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE users (
  user_id INT PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100),
  created_at TIMESTAMP
);
