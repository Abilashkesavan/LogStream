

# LogStream — Real-Time CDC Pipeline

**LogStream** is a fully functional, log-based Change Data Capture (CDC) pipeline that captures changes from a MySQL database and reliably replicates them to a target database in real time. It includes **updates, deletes,  idempotent writes**, making it production-ready.

---

## 🔹 Features

* Real-time CDC using **Debezium + Kafka**
* Source: **MySQL** with binlog enabled
* Target: **Postgres/MySQL** warehouse simulation
* Handles **inserts, updates, deletes, and snapshots**
* **Schema evolution support** (dynamic column handling)
* **Idempotent UPSERTs** to prevent duplicates during replays
* Resilient to **consumer crashes and Kafka restarts**
* **Monitoring & alerts** via Prometheus + Grafana
* Replay and offset-reset scripts for recovery

---

## 📂 Project Structure

```
LogStream/
├── debezium/
│   └── mysql-connector.json        # Debezium connector configuration
├── generator/
│   └── data_generator.py           # Generates inserts/updates/deletes
├── mysql/
│   └── init.sql                     # Source DB schema
├── target-db/
│   └── init.sql                     # Target DB schema
├── consumer/
│   ├── orders_consumer.py           # Orders table consumer
│   └── users_consumer.py            # Users table consumer
├── validation/
│   └── verify_counts.sql            # Verify source vs target counts
├── docker-compose.yml
└── README.md
```

---

## ⚙️ Setup & Usage

### 1. Clone the repository

```bash
git clone <repo-url>
cd LogStream
```

### 2. Start Docker services

```bash
docker-compose up -d
```

* MySQL (source) on port **3007**
* Kafka + Zookeeper
* Debezium Connect
* Target database (Postgres/MySQL)

### 3. Initialize the Debezium connector

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @debezium/mysql-connector.json
```

### 4. Start generating data

```bash
python generator/data_generator.py
```

### 5. Start consumers

```bash
python consumer/orders_consumer.py
python consumer/users_consumer.py
```

---

## ✅ Validation

* Compare **source vs target counts**:

```bash
docker exec -i mysql mysql -uroot -proot shop < validation/verify_counts.sql
```

* Test **insert, update, delete propagation**
* Test **consumer crash & replay recovery**
---

## 💡 Notes

* Consumers are **idempotent**, safe to restart
* Schema changes in source DB are **handled dynamically**
* Supports multiple tables and Kafka topics
* Dead-letter queue captures malformed events for review
---

## 📝 Author

**Abilash** – Data Engineering | Real-time CDC pipelines | Python & Kafka

---
