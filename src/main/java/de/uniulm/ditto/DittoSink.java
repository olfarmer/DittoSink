package de.uniulm.ditto;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.eclipse.ditto.client.DisconnectedDittoClient;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.MessagingProvider;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.eclipse.ditto.things.model.ThingId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Connector(
        name = "ditto-sink",
        type = IOType.SINK,
        help = "This Sink can be use to route messages from Pulsar to the Eclipse Ditto live messages. The necessary " +
                "metadata has to be set in the record properties.",
        configClass = DittoSinkConfig.class)
public class DittoSink implements Sink<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(DittoSink.class);

    private DittoClient dittoClient;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        DittoSinkConfig dittoSinkConfig = DittoSinkConfig.load(config);

        dittoClient = openDittoClient(dittoSinkConfig);
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        Map<String, String> properties = record.getProperties();
        String messageId = record.getMessage().isPresent() ? record.getMessage().get().getMessageId().toString() : "";

        if (!allRequiredPropertiesPresent(properties)) {
            logger.warn("Ignoring record with id {}", messageId);
            return;
        }


        ThingId thingId = ThingId.of(properties.get(RequiredProperties.THING_ID.propertyName));
        String featureId = properties.get(RequiredProperties.FEATURE_ID.propertyName);
        String subject = properties.get(RequiredProperties.SUBJECT.propertyName);
        OffsetDateTime eventTimeOffset = OffsetDateTime.of(LocalDateTime.ofEpochSecond(record.getEventTime().orElse(0L), 0, ZoneOffset.UTC), ZoneOffset.UTC);

        dittoClient
                .live()
                .message()
                .from(thingId)
                .featureId(featureId)
                .subject(subject)
                .timestamp(eventTimeOffset)
                .payload(ByteBuffer.wrap(record.getValue()))
                .contentType("application/octet-stream")
                .send();
    }

    @Override
    public void close() throws Exception {
        if(dittoClient != null) {
            dittoClient.destroy();
        }
    }

    private DittoClient openDittoClient(DittoSinkConfig config) {
        var authentication = AuthenticationProviders.basic(BasicAuthenticationConfiguration
                .newBuilder()
                .username(config.dittoUsername)
                .password(config.dittoPassword)
                .build());

        MessagingProvider messagingProvider = MessagingProviders.webSocket(
                WebSocketMessagingConfiguration
                        .newBuilder()
                        .endpoint(config.websocketEndpoint)
                        .build(), authentication);


        DisconnectedDittoClient disconnectedDittoClient = DittoClients.newInstance(messagingProvider);

        CompletableFuture<DittoClient> dittoClientCompletableFuture = new CompletableFuture<>();

        try {
            disconnectedDittoClient.connect()
                    .thenAccept(dittoClient -> {
                        dittoClientCompletableFuture.complete(dittoClient);
                        logger.info("DittoClient connected");
                    })
                    .exceptionally(error -> {
                        dittoClientCompletableFuture.completeExceptionally(error);
                        logger.error("Error connecting to DittoClient", error);
                        throw new RuntimeException(error);
                    }).toCompletableFuture().get();

            return dittoClientCompletableFuture.get();
        } catch (Exception e) {
            logger.error("Error connecting to DittoClient", e);
            throw new RuntimeException(e);
        }
    }

    private boolean allRequiredPropertiesPresent(Map<String, String> properties) {
        for (RequiredProperties requiredProperty : RequiredProperties.values()) {
            if (!properties.containsKey(requiredProperty.propertyName)) {
                logger.error("Required property {} is not present", requiredProperty);
                return false;
            }
        }
        return true;
    }

}
