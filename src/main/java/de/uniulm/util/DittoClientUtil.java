package de.uniulm.util;

import org.eclipse.ditto.client.DisconnectedDittoClient;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.MessagingProvider;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

    // Copied from DittoProject
    public static CompletableFuture<Boolean> createPolicyFromURL(DittoClient dittoClient, String policyURL) {
        var future = new CompletableFuture<Boolean>();

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(policyURL))
                .build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .exceptionally(e -> {
                    logger.error(e.getMessage(), e);
                    return null;
                })
                .thenApply(HttpResponse::body)
                .thenAccept(jsonString -> {
                    logger.info("Fetched policy from URL");
                    JsonObject policyJson = JsonFactory.readFrom(jsonString).asObject();
                    dittoClient.policies().create(policyJson)
                            .thenAccept(policy -> {
                                logger.info("Policy created: {}", policy);
                                future.complete(true);
                            })
                            .exceptionally(ex -> {
                                logger.error("Error: {}", ex.getMessage());
                                future.completeExceptionally(ex);
                                return null;
                            });
                });
        return future;

    }
}
