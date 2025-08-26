# DittoSink

An Apache Pulsar Eclipse Ditto sink.

## Setup
1. Build project and move .nar to /resources/pulsar/connectors
2. Run the docker compose file
3. Adjust the config in /resources/pulsar/connectors/DittoSinkConfig.yaml
4. Inside the broker container run

```pulsar-admin sinks create --name ditto-sink --classname de.uniulm.ditto.DittoSink --archive file:///pulsar/connectors/DittoSink-0.1.2-BETA.nar --inputs test --sink-config-file /pulsar/connectors/DittoSinkConfig.yaml```

to create the ditto sink

5. A test message can be produced by using this command:
   ```pulsar-client produce test -m 123 -p "featureId=test:test" -p "thingId=test" -p "subject=test"```

## Requirements for the messages

The following properties are required for messages processed in this sink:

1. thingId: The ID of the thing from which the message will be published. (REQUIRED)
2. subject: The message subject. (REQUIRED)
3. featureId: The feature ID of the thing with which this message should be associated. (OPTIONAL)

Provided the requirements are met, the message will be sent via the 'From' channel.
For example, the message could be received as follows:

```
...
client.live().registerForMessage("ditto-utility", "test", repliableMessage -> {
    String messagePayload = StandardCharsets.UTF_8.decode(repliableMessage.getRawPayload().orElseThrow()).toString();
    logger.info("Received message from {}: {}", repliableMessage.getEntityId(), messagePayload);
});
client.live().startConsumption();
```