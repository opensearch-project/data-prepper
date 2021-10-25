package org.opensearch.dataprepper.logstash.model;

/**
 * Types of attribute values in Logstash configuration
 *
 * @since 1.2
 */
public enum LogstashValueType {
    STRING,
    NUMBER,
    BAREWORD,
    ARRAY,
    HASH,
    PLUGIN
}
