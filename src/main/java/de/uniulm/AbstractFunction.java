package de.uniulm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public abstract class AbstractFunction {

    private final Logger logger = LoggerFactory.getLogger(AbstractFunction.class);
    private final List<String> requiredProperties;

    public AbstractFunction(List<String> requiredProperties) {
        this.requiredProperties = requiredProperties;
    }

    protected boolean allRequiredPropertiesPresent(Set<String> setProperties) {

        for (String requiredProperty : requiredProperties) {
            if (!setProperties.contains(requiredProperty)) {
                logger.error("Required property {} is not present", requiredProperty);
                return false;
            }
        }
        return true;
    }
}
