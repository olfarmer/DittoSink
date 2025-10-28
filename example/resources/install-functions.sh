#!/usr/bin/env bash
#
# deploy-pulsar.sh
#
# A simple script to create Pulsar functions and sinks, and to produce an example message.

set -euo pipefail



NAR_PATH="/pulsar/connectors/DittoSink-0.1.2-BETA.nar"
SINK_CONFIG="/pulsar/connectors/DittoSinkConfig.yaml"


FUNC1_NAME="ditto-event-management"
FUNC1_CLASS="de.uniulm.management.DittoEventManagement"

FUNC2_NAME="byte-to-json-processor"
FUNC2_CLASS="de.uniulm.processor.ByteToJsonProcessor"

SINK_NAME="ditto-sink"
SINK_CLASS="de.uniulm.ditto.DittoSink"

SINK_NAME2="influxdb-sink"

# Topics / Inputs / Outputs
FUNC1_INPUT="thingIds"
FUNC2_TOPIC_PATTERN="persistent://public/default/mqtt-.*"
FUNC2_OUTPUT="scalar-values"
SINK_INPUT="scalar-values"
PRODUCE_TOPIC="thingIds"
PRODUCE_MESSAGE="ma-pulsar-kafka:febr-9401"

# User-config for the first function (JSON string)
FUNC1_USER_CONFIG='{
  "dittoUsername": "ditto",
  "dittoPassword": "ditto",
  "websocketEndpoint": "ws://nginx:80/ws/2",
  "thingIds": ""
}'

# ---------------------------
# Create Pulsar Function: Ditto Event Management
# ---------------------------
echo "Creating function: $FUNC1_NAME"
pulsar-admin functions create \
  --name "$FUNC1_NAME" \
  --classname "$FUNC1_CLASS" \
  --jar "$NAR_PATH" \
  --inputs "$FUNC1_INPUT" \
  --user-config "$FUNC1_USER_CONFIG"

# ---------------------------
# Create Pulsar Function: Byte to JSON Processor
# ---------------------------
echo "Creating function: $FUNC2_NAME"
pulsar-admin functions create \
  --name "$FUNC2_NAME" \
  --classname "$FUNC2_CLASS" \
  --jar "$NAR_PATH" \
  --topics-pattern "$FUNC2_TOPIC_PATTERN" \
  --output "$FUNC2_OUTPUT"

# ---------------------------
# Create Pulsar Sink: Ditto Sink
# ---------------------------
echo "Creating sink: $SINK_NAME"
pulsar-admin sinks create \
  --name "$SINK_NAME" \
  --classname "$SINK_CLASS" \
  --archive "file://$NAR_PATH" \
  --inputs "$SINK_INPUT" \
  --sink-config-file "$SINK_CONFIG"

# ---------------------------
# Create Pulsar Sink: Influxdb Sink
# ---------------------------
echo "Creating sink: $SINK_NAME2"
pulsar-admin sinks create --name influxdb-sink --archive file:///pulsar/connectors/pulsar-io-influxdb-4.1.0.nar --inputs influxdb-record --sink-config-file /pulsar/connectors/InfluxdbSinkConfig.yaml --timeout-ms 180000


echo "All done!"