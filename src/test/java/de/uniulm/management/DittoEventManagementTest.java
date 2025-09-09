package de.uniulm.management;

import org.apache.pulsar.functions.api.Context;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DittoEventManagementTest {

    @Mock
    private Context mockContext = mock(Context.class, Answers.RETURNS_DEEP_STUBS);

    // Requires eclipse ditto to be running
    @Test
    void process() throws Exception {

        Map<String, Object> properties = new HashMap<>();
        properties.put("dittoUsername", "ditto");
        properties.put("dittoPassword", "ditto");
        properties.put("websocketEndpoint", "ws://localhost:8080/ws/2");
        properties.put("thingIds", ""); // No initial things. Later tested by explicitly calling .process()

        when(mockContext.getUserConfigMap()).thenReturn(properties);

        var driver = new DittoEventManagement();
        driver.initialize(mockContext);
        driver.process("ma-pulsar-kafka:febr-9401", mockContext);
    }
}