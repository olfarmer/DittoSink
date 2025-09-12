package de.uniulm.ditto;

import de.uniulm.AbstractFunction;
import de.uniulm.util.DittoClientUtil;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.twin.TwinFeatureHandle;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.wot.model.DataSchemaType;
import org.eclipse.ditto.wot.model.ThingDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Connector(
        name = "ditto-sink",
        type = IOType.SINK,
        help = "This Sink can be use to update attributes in features of Eclipse Ditto things. The necessary " +
                "metadata has to be set in the record properties.",
        configClass = DittoSinkConfig.class)
public class DittoSink extends AbstractFunction implements Sink<String> {

    private static final Logger logger = LoggerFactory.getLogger(DittoSink.class);

    private DittoClient dittoClient;

    Map<PropertyIdentifier, DataSchemaType> schemaMap = new HashMap<>();

    public DittoSink() {
        super(Arrays.stream(DittoSinkRequiredProperties.values()).map(x -> x.propertyName).toList());
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        DittoSinkConfig dittoSinkConfig = DittoSinkConfig.load(config);

        dittoClient = DittoClientUtil.openDittoClient(dittoSinkConfig.dittoUsername, dittoSinkConfig.dittoPassword, dittoSinkConfig.websocketEndpoint);
    }

    @Override
    public void write(Record<String> record) throws Exception {
        logger.info("Received record with value {}", record.getValue());
        Map<String, String> properties = record.getProperties();
        String messageId = record.getMessage().isPresent() ? record.getMessage().get().getMessageId().toString() : "";

        if (!allRequiredPropertiesPresent(properties.keySet())) {
            logger.warn("Ignoring record with id {}", messageId);
            return;
        }


        ThingId thingId = ThingId.of(properties.get(DittoSinkRequiredProperties.THING_ID.propertyName));
        String featureId = properties.get(DittoSinkRequiredProperties.FEATURE_ID.propertyName);
        String property = properties.get(DittoSinkRequiredProperties.PROPERTY.propertyName);

        PropertyIdentifier identifier = new PropertyIdentifier(thingId, featureId, property);
        DataSchemaType schema;

        if (schemaMap.containsKey(identifier)) {
            schema = schemaMap.get(identifier);
        } else {
            schema = getFeatureType(thingId, featureId, property);
        }

        TwinFeatureHandle handle = dittoClient
                .twin()
                .forId(thingId)
                .forFeature(featureId);

        logger.info("Updating feature {} of thing {} by putting value {}", featureId, thingId, record.getValue());

        CompletionStage<Void> stage = putProperty(handle, property, schema, record.getValue());

        stage.whenComplete((a, error) -> {
            if (error != null) {
                logger.error("Error occurred while trying to update the property {} in Feature {} of Thing {}", property, featureId, thingId, error);
                record.fail();
            } else {
                logger.info("Update successful");
                record.ack();
            }
        });
    }

    private CompletionStage<Void> putProperty(TwinFeatureHandle handle, String property, DataSchemaType schema, String value) {
        return switch (schema) {
            case BOOLEAN -> handle.putProperty(property, Boolean.parseBoolean(value));
            case INTEGER -> handle.putProperty(property, Integer.parseInt(value));
            case STRING -> handle.putProperty(property, value);
            case OBJECT, ARRAY -> handle.putProperty(property, JsonValue.of(value));
            case NUMBER -> handle.putProperty(property, Double.parseDouble(value));
            case NULL -> {
                logger.warn("Setting '' value for property {} as property DataSchemaType is NULL", property);
                yield handle.putProperty(property, "");
            }
        };
    }

    private DataSchemaType getFeatureType(ThingId thingId, String featureId, String propertyName) {

        CompletableFuture<DataSchemaType> returnValue = new CompletableFuture<>();

        dittoClient.twin().forId(thingId).forFeature(featureId).retrieve().whenComplete((feature, y) -> {
            String url = feature.getDefinition().orElseThrow().getFirstIdentifier().getUrl().orElseThrow().toString();
            //url = url.replace("nginx:80","localhost:8080"); // TODO: REMOVE!

            try {
                String jsonString = IOUtils.toString(new URL(url), StandardCharsets.UTF_8);
                var description = ThingDescription.fromJson(JsonObject.of(jsonString));
                var wotProperty = description.getProperties().orElseThrow().getProperty(propertyName).orElseThrow();

                DataSchemaType schema = wotProperty.getType().orElseThrow();
                returnValue.complete(schema);
            } catch (IOException e) {
                returnValue.completeExceptionally(e);
            }

        });

        return returnValue.join();
    }

    @Override
    public void close() throws Exception {
        if(dittoClient != null) {
            dittoClient.destroy();
        }
    }

}
