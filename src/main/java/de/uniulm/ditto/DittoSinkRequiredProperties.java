package de.uniulm.ditto;

public enum DittoSinkRequiredProperties {
    THING_ID("thingId"),
    FEATURE_ID("featureId"),
    PROPERTY("property");



    public final String propertyName;

    private DittoSinkRequiredProperties(String propertyName) {
        this.propertyName = propertyName;
    }
}
