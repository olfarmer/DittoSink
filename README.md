# DittoSink

An Apache Pulsar Eclipse Ditto sink. It accepts strings that are handled as attribute updates of a thing feature.
ThingId, FeatureId and property name have to be provided in the message properties.

## Setup

1. Build project and move .nar to example/resources/pulsar/connectors
2. Run the docker compose file
3. Adjust the config in example/resources/pulsar/connectors/DittoSinkConfig.yaml
4. Inside the broker container run

```pulsar-admin sinks create --name ditto-sink --classname de.uniulm.ditto.DittoSink --archive file:///pulsar/connectors/DittoSink-0.1.2-BETA.nar --inputs test --sink-config-file /pulsar/connectors/DittoSinkConfig.yaml```

to create the ditto sink

5. Create a thing in Ditto
6. A test update can be produced by using this command:
   ```pulsar-client produce theromometerTopic -m 12.4 -p "thingId=factory:theromometer" -p "featureId=temperature" -p "property=degrees"```

## Requirements for the messages

The following Pulsar message properties are required for an update to be processed in this sink:

1. thingId: The ID of the thing of which the attribute will be updated. (REQUIRED)
2. featureId: The feature ID of the thing with which this message should be associated. (REQUIRED)
3. property: The name of the property inside the feature that will be updated. (REQUIRED)



## Sink Config

On creation, sink config can be passed to the sink. The available options are:

* dittoUsername: BasicAuth ditto username
* dittoPassword: BasicAuth ditto password
* websocketEndpoint: ditto websocket endpoint, including protocol, port, and server path. Example: "ws://localhost:
  80/ws/2"

Currently, for authentication only BasicAuth is supported.

## Versions

This sink has been tested with Apache Pulsar version 4.0.4 and Eclipse Ditto 3.7.0