# DittoSink

An Apache Pulsar Eclipse Ditto sink.

## Setup
1. Run the docker compose file
2. Adjust the config in /resources/pulsar/connectors/DittoSinkConfig.yaml
3. Inside the broker container run

```pulsar-admin sinks create --name ditto-sink --classname de.uniulm.ditto.DittoSink --archive file:///pulsar/connectors/DittoSink-1.0-SNAPSHOT.nar --inputs test --sink-config-file /pulsar/connectors/DittoSinkConfig.yaml```

to create the ditto sink

4. A test message can be produced by using this command: ```pulsar-client produce test -m 123 -p "featureId=test" -p "thingId=test" -p "subject=test"```