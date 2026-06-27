#!/bin/bash
set -e

echo "Attente de la disponibilite de Kafka Connect sur http://localhost:8083 ..."
until curl -s -o /dev/null http://localhost:8083/connectors; do
  sleep 2
done
echo "Kafka Connect est pret."

echo ""
echo "Enregistrement du connecteur source (Debezium / CDC PostgreSQL)..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @connectors/source-connector.json \
  http://localhost:8083/connectors
echo ""

echo "Enregistrement du connecteur sink (JDBC vers PostgreSQL)..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @connectors/sink-connector.json \
  http://localhost:8083/connectors
echo ""

echo ""
echo "Verifie le statut des connecteurs avec :"
echo "  curl http://localhost:8083/connectors/orders-source/status"
echo "  curl http://localhost:8083/connectors/orders-sink/status"
