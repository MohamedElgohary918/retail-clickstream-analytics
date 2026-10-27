# Configure RSA Key-Pair Authentication for Snowflake Kafka Connect

This guide configures RSA key-pair authentication for the Snowflake
Kafka Sink Connector in the Retail Clickstream Analytics project.

> **Security:** Never commit the private key or
> `connector-secrets.properties` to GitHub, and never share their
> contents.

## 1. Create a secure directory

Run on the Ubuntu host:

``` bash
mkdir -p ~/.config/retail-clickstream/snowflake
chmod 700 ~/.config/retail-clickstream/snowflake
```

## 2. Generate an RSA key pair

Generate a 2048-bit RSA private key in unencrypted PKCS#8 format:

``` bash
openssl genpkey \
  -algorithm RSA \
  -out ~/.config/retail-clickstream/snowflake/rsa_key.p8 \
  -pkeyopt rsa_keygen_bits:2048
```

Generate the public key:

``` bash
openssl pkey \
  -in ~/.config/retail-clickstream/snowflake/rsa_key.p8 \
  -pubout \
  -out ~/.config/retail-clickstream/snowflake/rsa_key.pub
```

Set permissions and validate the private key:

``` bash
chmod 600 ~/.config/retail-clickstream/snowflake/rsa_key.p8
chmod 644 ~/.config/retail-clickstream/snowflake/rsa_key.pub

openssl pkey \
  -in ~/.config/retail-clickstream/snowflake/rsa_key.p8 \
  -check -noout
```

Expected output: `Key is valid`.

Keep `rsa_key.p8` secret. `rsa_key.pub` is the public key registered
with Snowflake.

## 3. Register the public key in Snowflake

Print the public key body without its PEM header and footer:

``` bash
sed '/-----BEGIN PUBLIC KEY-----/d; /-----END PUBLIC KEY-----/d' \
  ~/.config/retail-clickstream/snowflake/rsa_key.pub
```

Copy the output. In a Snowflake worksheet, run the following SQL,
replacing the placeholder with the copied public key body:

``` sql
ALTER USER RETAIL_KAFKA_CONNECT_USER
SET RSA_PUBLIC_KEY = 'PASTE_PUBLIC_KEY_BODY_HERE';
```

Do not include the `BEGIN PUBLIC KEY` or `END PUBLIC KEY` lines. This
must be run by a role authorized to alter this user.

Verify the registration:

``` sql
DESC USER RETAIL_KAFKA_CONNECT_USER;
```

Check the `RSA_PUBLIC_KEY_FP` property.

Reference: [Snowflake key-pair
authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth).

## 4. Create the connector secrets file

The connector configuration reads the key from
`/secrets/snowflake/connector-secrets.properties`. Your Compose file
mounts the host secrets directory at `/secrets/snowflake` in the
container.

Create the properties file on the host:

``` bash
KEY_DIR="$HOME/.config/retail-clickstream/snowflake"

{
  printf 'snowflake.private.key='
  sed '/-----BEGIN PRIVATE KEY-----/d; /-----END PRIVATE KEY-----/d' \
    "$KEY_DIR/rsa_key.p8" | tr -d '\n'
  printf '\n'
} > "$KEY_DIR/connector-secrets.properties"

chmod 600 "$KEY_DIR/connector-secrets.properties"
```

This stores the private key body on one line without the PEM header and
footer. Do not print or share the file contents.

Verify only that the file exists:

``` bash
test -f "$HOME/.config/retail-clickstream/snowflake/connector-secrets.properties" \
  && echo "Secrets file exists"
```

## 5. Confirm Docker Compose settings

In `streaming/kafka/compose.yaml`, the `connect` service should have the
following read-only mount (use your actual host username/path):

``` yaml
volumes:
  - /home/YOUR_LINUX_USERNAME/.config/retail-clickstream/snowflake:/secrets/snowflake:ro
```

The existing `connect` environment should enable the file configuration
provider:

``` yaml
CONNECT_CONFIG_PROVIDERS: file
CONNECT_CONFIG_PROVIDERS_FILE_CLASS: org.apache.kafka.common.config.provider.FileConfigProvider
```

Do not create a duplicate `volumes` key; add the mount to the existing
list.

## 6. Configure the Snowflake sink connector

In `streaming/config/snowflake-sink.json`, make sure the following
authentication properties appear inside the existing `config` object:

``` json
{
  "snowflake.url.name": "https://YOUR_ACCOUNT_IDENTIFIER.snowflakecomputing.com",
  "snowflake.user.name": "RETAIL_KAFKA_CONNECT_USER",
  "snowflake.role.name": "RETAIL_KAFKA_CONNECT_ROLE",
  "snowflake.authenticator": "snowflake_jwt",
  "snowflake.database.name": "RETAIL_CLICKSTREAM_DB",
  "snowflake.schema.name": "RAW",
  "snowflake.private.key": "${file:/secrets/snowflake/connector-secrets.properties:snowflake.private.key}"
}
```

This is an excerpt, not a complete connector definition. Preserve the
other existing settings, such as `connector.class`, `topics`,
converters, and topic-to-table mapping. Keep your actual Snowflake
account URL instead of the placeholder.

Reference: [Snowflake Kafka Connector
setup](https://docs.snowflake.com/en/user-guide/kafka-connector/setup-kafka).

## 7. Validate and restart Kafka Connect

Validate JSON syntax:

``` bash
python -m json.tool streaming/config/snowflake-sink.json
```

Restart Connect:

``` bash
docker compose -f streaming/kafka/compose.yaml restart connect
```

Review logs for errors:

``` bash
docker logs --tail 100 retail-connect
```

Do not share logs if they contain credentials or sensitive information.

## 8. Create or update the connector

List registered connectors:

``` bash
curl -sS http://localhost:8083/connectors
```

### If the connector already exists

Install `jq` if needed, then update its configuration:

``` bash
jq '.config' streaming/config/snowflake-sink.json > /tmp/snowflake-sink-config.json

curl -sS -X PUT \
  -H "Content-Type: application/json" \
  --data @/tmp/snowflake-sink-config.json \
  http://localhost:8083/connectors/RETAIL_CLICKSTREAM_SNOWFLAKE_SINK/config
```

### If the connector does not exist

Create it:

``` bash
curl -sS -X POST \
  -H "Content-Type: application/json" \
  --data @streaming/config/snowflake-sink.json \
  http://localhost:8083/connectors
```

## 9. Verify status

``` bash
curl -sS \
  http://localhost:8083/connectors/RETAIL_CLICKSTREAM_SNOWFLAKE_SINK/status
```

The connector and its task should report `RUNNING`. If authentication
fails, check:

-   The public key is registered to `RETAIL_KAFKA_CONNECT_USER`.
-   The connector uses that same username.
-   The private key is valid PKCS#8 PEM.
-   `connector-secrets.properties` exists and contains the expected
    property name.
-   The mount points to the correct host directory and is readable in
    the container.
-   The connector uses `snowflake.authenticator: snowflake_jwt`.
-   The configured role has access to the database, schema, and target
    table.

Finally, verify that events arrive in
`RETAIL_CLICKSTREAM_DB.RAW.RAW_CLICKSTREAM_EVENTS`.
