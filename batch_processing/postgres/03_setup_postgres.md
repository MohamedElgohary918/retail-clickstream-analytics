## Setup OLTP Database 

**Step 1 —Pull the PostgreSQL image**
We'll use the official PostgreSQL image with **PostgreSQL 17**.
Run:

```
docker pull postgres:17
```

This downloads the image without starting a container.

**Step 2 —Start PostgreSQL using Docker**
run:

```
docker run -d \
  --name retail-postgres \
  -e POSTGRES_USER=retail_admin \
  -e POSTGRES_PASSWORD='admin1234' \
  -e POSTGRES_DB=retail_oltp \
  -p 5050:5432 \
  -v retail_pgdata:/var/lib/postgresql/data \
  postgres:17
```

| Setting           | Purpose                                          |
| ----------------- | ------------------------------------------------ |
| `retail-postgres` | Container name                                   |
| `retail_admin`    | PostgreSQL user                                  |
| `retail_oltp`     | Database for your OLTP tables                    |
| `5432:5432`       | Exposes PostgreSQL on port 5432 of your computer |
| `retail_pgdata`   | Persistent Docker volume for database files      |
| `postgres:17`     | PostgreSQL 17 image                              |
**Verify that PostgreSQL is ready**
Run:

```
docker exec -it retail-postgres \
  pg_isready -U retail_admin -d retail_oltp
```

Expected output:
```
/var/run/postgresql:5432 - accepting connections
```

```
# Open a Bash shell inside the container

docker exec -it retail-postgres bash

# Then connect to PostgreSQL:

psql -U retail_admin -d retail_oltp
```

**Step 3 — Create the tables**
At the `retail_oltp=#` prompt, execute the following SQL.

```
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    country VARCHAR(100),
    registration_date TIMESTAMP
);

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category_id BIGINT,
    category_code TEXT,
    brand VARCHAR(255),
    price NUMERIC(12, 2),
    cost NUMERIC(12, 2)
);

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(customer_id),
    order_timestamp TIMESTAMP,
    order_total NUMERIC(14, 2),
    order_status VARCHAR(50)
);

CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id TEXT REFERENCES orders(order_id),
    product_id BIGINT REFERENCES products(product_id),
    quantity INTEGER,
    unit_price NUMERIC(12, 2),
    line_total NUMERIC(14, 2),
    first_purchase_time TIMESTAMP,
    UNIQUE (order_id, product_id)
);

CREATE TABLE inventory (
    product_id BIGINT PRIMARY KEY REFERENCES products(product_id),
    stock_quantity INTEGER,
    reorder_level INTEGER,
    inventory_origin VARCHAR(100)
);
```

**Step 4 —Create the destination directory inside the container**
Run this from your Ubuntu terminal:

```
docker exec retail-postgres mkdir -p /tmp/retail-processed
```

The syntax is:

```
docker cp <source> <destination>
```
This command **copies a file between your Ubuntu host and a Docker container**.

**Step 5 —Copy the files**

```
docker cp customers.csv retail-postgres:/tmp/retail-processed/
docker cp products.csv retail-postgres:/tmp/retail-processed/
docker cp orders.csv retail-postgres:/tmp/retail-processed/
docker cp order_items.csv retail-postgres:/tmp/retail-processed/
docker cp inventory.csv retail-postgres:/tmp/retail-processed/
```

**Step6 — Load CSV to retail_oltp Database**

Open PostgreSQL:
```
docker exec -it retail-postgres psql -U retail_admin -d retail_oltp
```

Run this command one by one.
```
\copy customers FROM '/tmp/retail-processed/customers.csv' WITH (FORMAT CSV, HEADER TRUE);
```

```
\copy products FROM '/tmp/retail-processed/products.csv' WITH (FORMAT CSV, HEADER TRUE);
```

```
\copy orders FROM '/tmp/retail-processed/orders.csv' WITH (FORMAT CSV, HEADER TRUE);
```

```
\copy order_items FROM '/tmp/retail-processed/order_items.csv' WITH (FORMAT CSV, HEADER TRUE);
```

```
\copy inventory FROM '/tmp/retail-processed/inventory.csv' WITH (FORMAT CSV, HEADER TRUE);
```

**Important:** Run each command separately and wait for its result before executing the next. If a table was already imported successfully, don't import it again.
