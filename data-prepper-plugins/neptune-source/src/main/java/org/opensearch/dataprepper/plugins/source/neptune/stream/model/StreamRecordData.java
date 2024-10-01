package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.LinkedHashMap;

// TODO: support both PG and SPARQL data
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecordData {

    private String id;

    private String type;

    private String key;

    private LinkedHashMap<String, String> value;

    private String from;

    private String to;

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public LinkedHashMap<String, String> getValue() {
        return value;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }
}
