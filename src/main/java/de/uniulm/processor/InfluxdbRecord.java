package de.uniulm.processor;

import java.util.Map;

public record InfluxdbRecord(String measurement, long timestamp, Map<String, String> tags, Map<String, Object> fields) {

}
