package de.uniulm.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.uniulm.AbstractFunction;
import de.uniulm.ditto.DittoSinkRequiredProperties;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ByteToJsonProcessor extends AbstractFunction implements Function<byte[], Void> {

    private static final Logger logger = LoggerFactory.getLogger(ByteToJsonProcessor.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public ByteToJsonProcessor() {
        super(Arrays.stream(ByteToJsonProcessorRequiredProperties.values()).map(x -> x.propertyName).toList());
    }

    private static Map<String, String> getPropertyNameToFeatureId(Map<String, String> properties, JsonNode node) {
        //Expected input format myJsonFieldName=myFeatureName;mySecondJsonFieldName=mySecondFeatureName
        String mappings = properties.get(ByteToJsonProcessorRequiredProperties.PROPERTY_FEATURE_MAPPING.propertyName);
        Map<String, String> propertyNameToFeatureId = new HashMap<>();

        for (var mapping : mappings.split(";")) {
            var splitMapping = mapping.split("=");

            if (splitMapping.length != 2) {
                logger.error("Invalid mapping: {}. Aborting.", mapping);
                return null;
            }
            propertyNameToFeatureId.put(splitMapping[0], splitMapping[1]);
        }
        return propertyNameToFeatureId;
    }

    // Converts the value of a JsonNode n to a String. If the value is another JsonObject, the object will be returned.
    private static String jsonNodeToString(JsonNode n) throws IOException {
        if (n == null || n.isNull()) {
            return null;
        }

        if (n.isValueNode()) {
            if (n.isTextual()) return n.textValue();
            if (n.isNumber()) return n.numberValue().toString();
            if (n.isBoolean()) return n.booleanValue() ? "true" : "false";
            if (n.isBinary()) return new String(n.binaryValue());
            return n.asText();
        } else {
            return mapper.writeValueAsString(n);
        }
    }

    @Override
    public void initialize(Context context) throws Exception {
        Function.super.initialize(context);
    }

    @Override
    public void close() throws Exception {
        Function.super.close();
    }

    @Override
    public Void process(byte[] input, Context context) throws Exception {
        Map<String, String> properties = context.getCurrentRecord().getProperties();
        JsonNode node;

        try {
            node = mapper.readTree(input);
        } catch (Exception e) {
            logger.error("Received a byte message that was not JSON", e);
            throw e;
        }

        if (!allRequiredPropertiesPresent(properties.keySet())) {
            logger.warn("Ignoring record");
            return null;
        }

        Map<String, String> propertyNameToFeatureId = getPropertyNameToFeatureId(properties, node);
        if (propertyNameToFeatureId == null) return null;

        // Each json property will be sent in a new message
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            String featureId = propertyNameToFeatureId.get(field.getKey());

            Map<String, String> outgoingMessageProperties = new HashMap<>();
            outgoingMessageProperties.put(DittoSinkRequiredProperties.PROPERTY.propertyName, field.getKey());
            outgoingMessageProperties.put(DittoSinkRequiredProperties.FEATURE_ID.propertyName, featureId);
            outgoingMessageProperties.put(DittoSinkRequiredProperties.THING_ID.propertyName, context.getCurrentRecord().getProperties().get(ByteToJsonProcessorRequiredProperties.THING_ID.propertyName));

            context.newOutputMessage(context.getOutputTopic(), Schema.STRING)
                    .value(jsonNodeToString(field.getValue()))
                    .properties(outgoingMessageProperties)
                    .sendAsync();
        }

        return null;
    }
}
