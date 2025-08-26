package de.uniulm.ditto;

public enum RequiredProperties {
    THING_ID("thingId"),
    SUBJECT("subject"),
    ;


    public final String propertyName;

    private RequiredProperties(String propertyName) {
        this.propertyName = propertyName;
    }
}
