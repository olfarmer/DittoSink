package de.uniulm.presentation;


import de.uniulm.util.DittoClientUtil;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingDefinition;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final DittoClient client = DittoClientUtil.openDittoClient(Config.DITTO_USERNAME, Config.DITTO_PASSWORD, Config.WEBSOCKET_ENDPOINT);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CompletableFuture<Void> done = new CompletableFuture<>();


        createThing()
                .thenAccept(thing -> {
                            if (thing != null) {
                                logger.info("Thing created");

                                var features = thing.getFeatures().get();
                                logger.info("Features count: {}", features.getSize());
                            }


                            System.exit(0);
                            done.complete(null);
                        }
                );


        readMessages();

        done.get();
    }

    private static void readMessages() {


        client.live().registerForMessage("ditto-utility", "test", repliableMessage -> {
            String messagePayload = StandardCharsets.UTF_8.decode(repliableMessage.getRawPayload().orElseThrow()).toString();
            logger.info("Received message from {}: {}", repliableMessage.getEntityId(), messagePayload);
        });
        client.live().startConsumption();
    }

    private static CompletionStage<Thing> createThing() throws ExecutionException, InterruptedException {
        ThingDefinition thingDefinition = ThingsModelFactory.newDefinition("http://nginx:80/definitions/febr/febr-9401.jsonld");
        ThingId thingId = ThingId.of("ma-pulsar-kafka", "febr-9401");


        if (!policyExists("nginx:basic").get()) {
            DittoClientUtil.createPolicyFromURL(client, "http://localhost:8080/policies/nginx.jsonld")
                    .exceptionally(e -> {
                        logger.error("ERROR creating policy for Nginx: {}", e.getMessage());
                        return null;
                    })
                    .thenAccept(x -> {
                        logger.info("Creating policy for Nginx");
                    }).get();
        }

        Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setDefinition(thingDefinition)
                .setPolicyId(PolicyId.of("nginx:basic"))
                .build();

        return client.twin().create(thing).exceptionallyAsync(e -> {
            logger.error("Thing could not be created due to: {}", e.getMessage());
            return null;
        });
    }

    private static CompletableFuture<Boolean> policyExists(String policy) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        client
                .policies()
                .retrieve(PolicyId.of(policy))
                .thenApply(pol -> future.complete(true))
                .exceptionally(ex -> future.complete(false));
        return future;
    }
}