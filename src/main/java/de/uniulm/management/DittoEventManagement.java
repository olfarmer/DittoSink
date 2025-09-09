package de.uniulm.management;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.uniulm.AbstractFunction;
import de.uniulm.util.DittoClientUtil;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.wot.model.Events;
import org.eclipse.ditto.wot.model.ThingDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DittoEventManagement extends AbstractFunction implements Function<String, Void> {

    private static final Logger logger = LoggerFactory.getLogger(DittoEventManagement.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final List<String> subscribedThingIds = new ArrayList<>();
    private DittoClient dittoClient;
    private Context context;

    public DittoEventManagement() {
        super(new ArrayList<>());
    }

    @Override
    public Void process(String input, Context context) throws Exception {
        try {
            CreateTopologyForThing(input);
        } catch (Exception e) {
            logger.error("Could not create topology for thing {}", input, e);
        }

        return null;
    }

    @Override
    public void initialize(Context context) throws Exception {
        Function.super.initialize(context);
        this.context = context;

        Map<String, Object> config = context.getUserConfigMap();

        dittoClient = DittoClientUtil.openDittoClient(config.get("dittoUsername").toString(), config.get("dittoPassword").toString(), config.get("websocketEndpoint").toString());

        if (config.get("thingIds").toString().isEmpty()) {
            return;
        }

        String[] initialSubscriptions = config.get("thingIds").toString().split(",");


        for (String initialSubscription : initialSubscriptions) {
            try {
                CreateTopologyForThing(initialSubscription);
            } catch (Exception e) {
                logger.error("Could not create topology for thing {}", initialSubscription, e);
            }
        }

    }

    @Override
    public void close() throws Exception {
        Function.super.close();

        if (dittoClient != null) {
            dittoClient.destroy();
        }
    }

    private void CreateTopologyForThing(String stringThingId) throws ExecutionException, InterruptedException {
        ThingId thingId = ThingId.of(stringThingId);

        ThingDescription description = GetThingDescription(stringThingId, thingId).get();

        List<MqttConnection> connections = GetMqttConnectionsOfThing(description);


        List<String> distinctMqttUrls = connections.stream().map(MqttConnection::href).distinct().toList();

        for (String connection : distinctMqttUrls) {
            try {
                CreateMqttSourceForThingIfNotExists(connection, thingId);
            } catch (Exception e) {
                logger.error("Could not create topology for thing {}", stringThingId, e);
            }
        }
    }

    private CompletableFuture<ThingDescription> GetThingDescription(String stringThingId, ThingId thingId) {
        CompletableFuture<ThingDescription> future = new CompletableFuture<>();

        dittoClient.twin().forId(thingId).retrieve().whenComplete((thing, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }

            try {
                String definitionUrl = thing.getDefinition().orElseThrow().getUrl().orElseThrow().toString();
                //TODO: ONLY FOR DEBUGGING
                definitionUrl = definitionUrl.replace("nginx:80", "localhost:8080");
                String jsonString = IOUtils.toString(new URL(definitionUrl), StandardCharsets.UTF_8);
                future.complete(ThingDescription.fromJson(JsonObject.of(jsonString)));
            } catch (Exception e) {
                logger.error("Error creating topology for thing {}", stringThingId, e);
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private void CreateMqttSourceForThingIfNotExists(String connectionUrl, ThingId thingId) throws PulsarAdminException, ExecutionException, InterruptedException, URISyntaxException {
        PulsarAdmin admin = context.getPulsarAdmin();
        String mqttName = "mqtt-" + thingId;

        if (admin.sources().listSources("public", "default").contains(mqttName)) {
            logger.info("Source {} already exists.", mqttName);
            return;
        }

        ObjectNode node = mapper.createObjectNode(); // TODO: mqtt source must support this
        node.put("thingId", thingId.toString());
        node.put("propertyFeatureMapping", GetPropertyFeatureMapping(thingId).get());

        Map<String, Object> customConfig = new HashMap<>();
        URI url = new URI(connectionUrl);
        customConfig.put("host", url.getHost());
        customConfig.put("port", url.getPort());
        customConfig.put("serverPath", url.getPath());
        customConfig.put("tls", "false");
        customConfig.put("websocket", "false");
        customConfig.put("topics", List.of("#"));
        customConfig.put("additionalProperties", node.toString());




        SourceConfig config = new SourceConfig();
        config.setConfigs(customConfig);
        config.setTenant("public");
        config.setNamespace("default");
        config.setName(mqttName + "-source");
        config.setClassName("de.exxcellent.orchideo.connect.pulsar.MqttSource"); // TODO: extract this to the config
        config.setArchive("file:///pulsar/connectors/MqttSource-1.0-SNAPSHOT.nar");
        config.setTopicName(mqttName);

        admin.sources().createSource(config, "file:///pulsar/connectors/MqttSource-1.0-SNAPSHOT.nar");
    }

    private CompletableFuture<String> GetPropertyFeatureMapping(ThingId thingId) {
        CompletableFuture<String> future = new CompletableFuture<>();

        dittoClient.twin().forId(thingId).retrieve().whenComplete((thing, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
                return;
            }

            StringBuilder mapping = new StringBuilder();
            Features features = thing.getFeatures().orElseThrow();

            for (Feature feature : features) {
                FeatureProperties properties = feature.getProperties().orElseThrow();
                List<String> stringProperties = properties.getKeys().stream().map(Object::toString).toList();

                for (String stringProperty : stringProperties) {
                    mapping.append(stringProperty).append("=").append(feature.getId()).append(";");
                }
            }

            future.complete(mapping.toString());
        });

        return future;
    }

    private void CreateByteToJsonProcessor(ThingId thingId) throws PulsarAdminException {
        FunctionConfig config = new FunctionConfig();
        config.setClassName("de.uniulm.processor.ByteToJsonProcessor");
        config.setInputs(List.of("mqtt-" + thingId));
        config.setOutput("properties-" + thingId);

        context.getPulsarAdmin().functions().createFunction(config, "file:///pulsar/connectors/ByteToJsonProcessor-1.0-SNAPSHOT.nar");
    }

    private List<MqttConnection> GetMqttConnectionsOfThing(ThingDescription description) {
        List<MqttConnection> connections = new ArrayList<>();

        Events events = description.getEvents().orElseThrow();

        for (String event : events.keySet()) {
            var forms = events.getEvent(event).orElseThrow().getForms().orElseThrow();

            for (var form : forms) {
                if (form.getSubprotocol().orElseThrow().toString().equalsIgnoreCase("mqtt")) {
                    connections.add(new MqttConnection(form.getHref().toString()));
                }
            }
        }

        return connections;
    }

}
