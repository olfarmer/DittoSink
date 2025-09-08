package de.uniulm.processor;

public enum ByteToJsonProcessorRequiredProperties {
    THING_ID("thingId"),
    PROPERTY_FEATURE_MAPPING("propertyFeatureMapping");


    public final String propertyName;

    ByteToJsonProcessorRequiredProperties(String propertyName) {
        this.propertyName = propertyName;
    }

}
