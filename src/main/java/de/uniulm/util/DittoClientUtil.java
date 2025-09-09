package de.uniulm.util;

import org.eclipse.ditto.client.DisconnectedDittoClient;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.MessagingProvider;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class DittoClientUtil {

    private static final Logger logger = LoggerFactory.getLogger(DittoClientUtil.class);

    public static DittoClient openDittoClient(String username, String password, String websocketEndpoint) {
        var authentication = AuthenticationProviders.basic(BasicAuthenticationConfiguration
                .newBuilder()
                .username(username)
                .password(password)
                .build());

        MessagingProvider messagingProvider = MessagingProviders.webSocket(
                WebSocketMessagingConfiguration
                        .newBuilder()
                        .endpoint(websocketEndpoint)
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
}
