package de.uniulm.processor;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ByteToJsonProcessorTest {

    @Mock
    private org.apache.pulsar.functions.api.Record<String> mockRecord = mock(Record.class);

    @Mock
    private Context mockContext = mock(Context.class);

    @Mock
    private TypedMessageBuilder<String> mockStringTypedMessageBuilder = mock(TypedMessageBuilder.class);

    @BeforeEach
    void setUp() {
    }

    @Test
    void process() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(ByteToJsonProcessorRequiredProperties.THING_ID.propertyName, "test:test");
        properties.put(ByteToJsonProcessorRequiredProperties.PROPERTY_FEATURE_MAPPING.propertyName, "myValue=feature1;mySecondValue=feature2");

        String jsonInput = "{\n" +
                "    \"myValue\": 12,\n" +
                "    \"mySecondValue\": \"anyValue\"\n" +
                "}";

        when(mockRecord.getMessage()).thenReturn(Optional.empty());
        when(mockRecord.getValue()).thenReturn(jsonInput);
        when(mockRecord.getProperties()).thenReturn(properties);

        when(mockContext.getCurrentRecord()).thenReturn((Record) mockRecord);
        when(mockContext.getOutputTopic()).thenReturn("topic");
        when(mockContext.<String>newOutputMessage(anyString(), eq(Schema.STRING)))
                .thenReturn(mockStringTypedMessageBuilder);

        when(mockStringTypedMessageBuilder.properties(any())).thenReturn(mockStringTypedMessageBuilder);
        when(mockStringTypedMessageBuilder.key(any())).thenReturn(mockStringTypedMessageBuilder);
        when(mockStringTypedMessageBuilder.eventTime(anyLong())).thenReturn(mockStringTypedMessageBuilder);
        when(mockStringTypedMessageBuilder.value(anyString())).thenReturn(mockStringTypedMessageBuilder);
        when(mockStringTypedMessageBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.latest));

        new ByteToJsonProcessor().process(jsonInput.getBytes(), mockContext);

        InOrder inOrder = inOrder(mockStringTypedMessageBuilder);

        // first record
        inOrder.verify(mockStringTypedMessageBuilder).value("12");
        inOrder.verify(mockStringTypedMessageBuilder).properties(argThat(props ->
                "myValue".equals(props.get("property")) &&
                        "feature1".equals(props.get("featureId")) &&
                        "test:test".equals(props.get("thingId"))));
        inOrder.verify(mockStringTypedMessageBuilder).sendAsync();

        // second record
        inOrder.verify(mockStringTypedMessageBuilder).value("anyValue");
        inOrder.verify(mockStringTypedMessageBuilder).properties(argThat(props ->
                "mySecondValue".equals(props.get("property")) &&
                        "feature2".equals(props.get("featureId")) &&
                        "test:test".equals(props.get("thingId"))));
        inOrder.verify(mockStringTypedMessageBuilder).sendAsync();


    }
}