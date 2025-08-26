# DittoSink

An Apache Pulsar Eclipse Ditto sink.

## Setup
1. Build project and move .nar to /resources/pulsar/connectors
2. Run the docker compose file
3. Adjust the config in /resources/pulsar/connectors/DittoSinkConfig.yaml
4. Inside the broker container run

```pulsar-admin sinks create --name ditto-sink --classname de.uniulm.ditto.DittoSink --archive file:///pulsar/connectors/DittoSink-0.1.1-BETA.nar --inputs test --sink-config-file /pulsar/connectors/DittoSinkConfig.yaml```

to create the ditto sink

5. A test message can be produced by using this command: ```pulsar-client produce test -m 123 -p "featureId=test" -p "thingId=test" -p "subject=test"```