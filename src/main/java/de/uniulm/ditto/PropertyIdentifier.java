package de.uniulm.ditto;

import org.eclipse.ditto.things.model.ThingId;

import java.util.Objects;

public class PropertyIdentifier {

    private final ThingId thingId;
    private final String propertyName;
    private final String featureId;

    public PropertyIdentifier(ThingId thingId, String featureId, String propertyName) {
        this.thingId = thingId;
        this.featureId = featureId;
        this.propertyName = propertyName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PropertyIdentifier that = (PropertyIdentifier) o;
        return Objects.equals(thingId, that.thingId) && Objects.equals(propertyName, that.propertyName) && Objects.equals(featureId, that.featureId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thingId, propertyName, featureId);
    }
}
