# Real-Time Clickstream Streaming Pipeline

## Overview

We simulate user activity by reading historical clickstream events and publishing them to Kafka at a controlled rate. Kafka acts as the event-streaming layer, and Kafka Connect delivers the events to Snowflake for downstream analytics.

**Architecture:**

`Python Producer → Apache Kafka → Kafka Connect + Snowflake Connector → Snowflake RAW`

The pipeline uses Docker Compose to run Kafka and Kafka Connect. Snowflake key-pair authentication secures the connection between Kafka Connect and Snowflake.

## Prerequisites

Before starting, ensure you have:

- Docker Engine or Docker Desktop with Docker Compose.
- Python and the project's virtual environment.
- The project repository cloned locally.
- Access to the Snowflake account and permission to configure the service user.
- The required project files, including the Dockerfile, Compose file, connector configuration, and producer script.

Run the following commands from the project root:

```bash
cd ~/WorkSpace/retail-clickstream-analytics
```

## Step 1 — Build the Kafka Connect image

Build the custom image containing the Snowflake Kafka Connector:

```bash
docker build \
  -t retail-snowflake-connect:local \
  -f streaming/connect/Dockerfile \
  streaming/connect
```

The Dockerfile uses the Confluent Kafka Connect base image and installs the Snowflake connector plugin.

This step builds the image; it does not start the containers.

## Step 2 — Configure Snowflake RSA key-pair authentication

The Snowflake connector authenticates using an RSA private key rather than a password.

The private key stays on your machine and is supplied securely to Kafka Connect. The corresponding public key is registered against the Snowflake service user.

### 2.1 Generate the key pair

Create a directory for the credentials:

```bash
mkdir -p ~/.config/retail-clickstream/snowflake
chmod 700 ~/.config/retail-clickstream/snowflake
```

Generate an unencrypted PKCS#8 private key and its public key:

```bash
openssl genrsa 2048 | openssl pkcs8 \
  -topk8 \
  -inform PEM \
  -outform PEM \
  -nocrypt \
  -out ~/.config/retail-clickstream/snowflake/rsa_key.p8

openssl pkey \
  -in ~/.config/retail-clickstream/snowflake/rsa_key.p8 \
  -pubout \
  -out ~/.config/retail-clickstream/snowflake/rsa_key.pub

chmod 600 ~/.config/retail-clickstream/snowflake/rsa_key.p8
```

The generated files are:

```text
~/.config/retail-clickstream/snowflake/
├── rsa_key.p8
└── rsa_key.pub
```

The private key is unencrypted in this setup, so protect it carefully. Never commit it to GitHub.

### 2.2 Register the public key in Snowflake

Display the public key without the PEM header and footer:

```bash
awk '!/-----/' ~/.config/retail-clickstream/snowflake/rsa_key.pub \
  | tr -d '\n'
```

Copy the output and execute the following SQL in Snowflake using a role authorized to modify the service user:

```sql
ALTER USER RETAIL_KAFKA_CONNECT_USER
SET RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>';
```

Replace the placeholder with the copied public key. Do not include the PEM header or footer.

The public key must belong to the same Snowflake user specified in the connector configuration.

### 2.3 Create the connector secrets file

Create the properties file containing the private key:

```bash
{
  printf 'snowflake.private.key='
  awk '!/-----/' \
    ~/.config/retail-clickstream/snowflake/rsa_key.p8 \
    | tr -d '\n'
  printf '\n'
} > ~/.config/retail-clickstream/snowflake/connector-secrets.properties

chmod 600 \
  ~/.config/retail-clickstream/snowflake/connector-secrets.properties
```

The file contains one property:

```properties
snowflake.private.key=<PRIVATE_KEY_CONTENT>
```

The placeholder is illustrative; the command generates the actual key content.

The Compose configuration must mount the credentials directory read-only:

```text
~/.config/retail-clickstream/snowflake
    → /secrets/snowflake:ro
```

The connector configuration must reference the property using:

```json
"snowflake.private.key": "${file:/secrets/snowflake/connector-secrets.properties:snowflake.private.key}"
```


> This setup assumes the Snowflake service user and required database permissions have already been configured.

## Step 3 — Start Kafka and Kafka Connect

Build any configured custom images and start the services:

```bash
docker compose \
  -f streaming/kafka/compose.yaml \
  up -d --build
```

The main services are:

| Container | Purpose | Local port |
|---|---|---|
| `retail-kafka` | Receives clickstream events | `9094` |
| `retail-connect` | Transfers Kafka events to Snowflake | `8083` |

Kafka Connect's REST API is available at `http://localhost:8083`.


## Step 4 — Create the Kafka topic

Topic auto-creation is disabled, so create the topic explicitly before running the producer:

```bash
docker exec retail-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic clickstream_events \
  --partitions 1 \
  --replication-factor 1
```

Verify that the topic exists:

```bash
docker exec retail-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected topic:

```text
clickstream_events
```

If the topic already exists, verify it rather than attempting to recreate it.

## Step 5 — Register the Snowflake sink connector

If the connector has not already been registered, submit its configuration to Kafka Connect:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @streaming/config/snowflake-sink.json
```

This registers the connector named `RETAIL_CLICKSTREAM_SNOWFLAKE_SINK`.

The configuration specifies the Kafka topic, Snowflake account, service user, database, schema, target table mapping, and private-key property.

If the connector is already registered, do not submit the same creation request again. Check its status instead.

## Step 6 — Verify the Snowflake connector

Check the connector and task status:

```bash
curl -s \
  http://localhost:8083/connectors/RETAIL_CLICKSTREAM_SNOWFLAKE_SINK/status \
  | python3 -m json.tool
```

You want both the connector and its task to report:

```json
"state": "RUNNING"
```

A running status indicates that the connector and task are running, but it does not by itself prove that events have reached Snowflake.


## Step 7 — Run or resume the producer

Activate the project's Python virtual environment:

```bash
cd ~/WorkSpace/retail-clickstream-analytics
source .venv/bin/activate
```

Run the producer:

```bash
python3 streaming/producer/clickstream_producer.py \
  --limit 1000 \
  --rate 100
```

- `--limit 1000`: Publishes up to 1,000 additional source rows during this run.
- `--rate 100`: Requests a target publishing rate of approximately 100 events per second. Actual throughput can be lower.

### Checkpoint behavior

The checkpoint is stored at:

```text
streaming/producer/checkpoint.json
```

The producer uses it to resume from the saved source position.

**Do not use `--reset-checkpoint` during a normal restart.**

The checkpoint tracks events acknowledged by Kafka, not events successfully ingested into Snowflake. Events already in Kafka may still be processed after the producer stops or Kafka Connect restarts.

Resetting the producer checkpoint does not clear Kafka or remove existing Snowflake rows.

## Step 8 — Verify the events in Snowflake

After publishing events, query the target table:

```sql
SELECT
    COUNT(*) AS TOTAL_EVENTS,
    MIN(EVENT_TIME) AS FIRST_EVENT_TIME,
    MAX(EVENT_TIME) AS LAST_EVENT_TIME
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS;
```

Inspect a sample of recent arrivals:

```sql
SELECT *
FROM RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS
ORDER BY SIMULATED_AT DESC
LIMIT 20;
```

Confirm that the event count increases and that the records contain the expected fields.

## Troubleshooting

### Kafka Connect is not running

```bash
docker logs --tail 100 retail-connect
```

### The connector is not registered

Register it using the command in Step 5.

### The connector is registered but its task has failed

Check the connector status and logs before restarting it. Fix the reported configuration, authentication, or permission issue first.

### Snowflake authentication fails

Check that:

- The public key was registered against `RETAIL_KAFKA_CONNECT_USER`.
- The private key corresponds to that public key.
- The secrets file exists at the mounted container path.
- The connector configuration references the correct property.
- The private key file has not been modified or exposed.

### Events appear in Kafka but not in Snowflake

Check the Kafka Connect task status and logs, then verify the Snowflake user permissions and target table configuration.

## Stop the services

To stop the services without removing their containers or persistent volumes:

```bash
docker compose -f streaming/kafka/compose.yaml stop
```

To start them again:

```bash
docker compose -f streaming/kafka/compose.yaml up -d
```

Persistent Kafka data and the producer checkpoint allow the pipeline to resume, subject to the configured retention and checkpoint behavior.