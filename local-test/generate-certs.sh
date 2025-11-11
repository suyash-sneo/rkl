#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/certs"
mkdir -p "$DIR"
cd "$DIR"

echo "Generating CA"
openssl genrsa -out ca.key 2048 >/dev/null 2>&1
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=Test CA" -out ca.crt >/dev/null 2>&1

echo "Generating server key/cert (CN=kafka, SAN=kafka,localhost)"
cat > server.ext <<EOF
subjectAltName = @alt_names
extendedKeyUsage = serverAuth
[alt_names]
DNS.1 = kafka
DNS.2 = localhost
EOF
openssl genrsa -out server.key 2048 >/dev/null 2>&1
openssl req -new -key server.key -subj "/CN=kafka" -out server.csr >/dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256 -extfile server.ext >/dev/null 2>&1

echo "Generating client key/cert (CN=rkl-client)"
cat > client.ext <<EOF
extendedKeyUsage = clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
EOF
openssl genrsa -out client.key 2048 >/dev/null 2>&1
openssl req -new -key client.key -subj "/CN=rkl-client" -out client.csr >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256 -extfile client.ext >/dev/null 2>&1

echo "Building PKCS12 and JKS keystore/truststore"
openssl pkcs12 -export -in server.crt -inkey server.key -name kafka -password pass:password -out kafka.p12 >/dev/null 2>&1

if command -v keytool >/dev/null 2>&1; then
  KT="keytool"
else
  echo "keytool not found; using Docker openjdk image to convert"
  KT="docker run --rm -v $PWD:/work -w /work openjdk:11-jre-slim keytool"
fi

eval "$KT -importkeystore -destkeystore kafka.keystore.jks -deststorepass password -destkeypass password -srckeystore kafka.p12 -srcstoretype PKCS12 -srcstorepass password -alias kafka -noprompt" >/dev/null 2>&1
eval "$KT -keystore kafka.truststore.jks -alias CARoot -import -file ca.crt -storepass password -noprompt" >/dev/null 2>&1

# Create a credentials file with the password used above so the Confluent image can read it
echo "password" > password.txt

echo "Done. Files in $DIR:"
ls -1

echo
echo "Use these PEMs in the app environment modal:"
echo "- CA:    $(pwd)/ca.crt"
echo "- Cert:  $(pwd)/client.crt"
echo "- Key:   $(pwd)/client.key"

# Create a client-ssl.properties for console consumers inside the container
cat > client-ssl.properties <<EOF
security.protocol=SSL
ssl.ca.location=/etc/kafka/secrets/ca.crt
ssl.certificate.location=/etc/kafka/secrets/client.crt
ssl.key.location=/etc/kafka/secrets/client.key
EOF
echo "Created client-ssl.properties for container usage."
