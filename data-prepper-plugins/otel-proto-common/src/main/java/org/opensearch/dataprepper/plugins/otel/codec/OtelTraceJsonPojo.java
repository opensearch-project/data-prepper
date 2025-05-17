package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OtelTraceJsonPojo {
    @JsonProperty("resourceSpans")
    public List<ResourceSpan> resourceSpans;


    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ResourceSpan {
        @JsonProperty("resource")
        public Resource resource;
        @JsonProperty("scopeSpans")
        public List<ScopeSpan> scopeSpans;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Resource {
        @JsonProperty("attributes")
        public List<Attribute> attributes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Attribute {
        @JsonProperty("key")
        public String key;

        @JsonProperty("value")
        public Value value;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Value {
        @JsonProperty("stringValue")
        public String stringValue;

        @JsonProperty("intValue")
        public Integer intValue;

        @JsonProperty("doubleValue")
        public Double doubleValue;

        @JsonProperty("boolValue")
        public Boolean boolValue;

        public Object getValue() {
            if (stringValue != null) return stringValue;
            if (intValue != null) return intValue;
            if (doubleValue != null) return doubleValue;
            if (boolValue != null) return boolValue;
            return null;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ScopeSpan {
        @JsonProperty("scope")
        public Scope scope;
        @JsonProperty("spans")
        public List<Span> spans;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Scope {
        @JsonProperty("name")
        public String name;
        @JsonProperty("version")
        public String version;
        @JsonProperty("attributes")
        public List<Attribute> attributes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Span {
        @JsonProperty("traceId")
        public String traceId;
        @JsonProperty("spanId")
        public String spanId;
        @JsonProperty("parentSpanId")
        public String parentSpanId;
        @JsonProperty("flags")
        public String flags;
        @JsonProperty("name")
        public String name;
        @JsonProperty("kind")
        public String kind;
        @JsonProperty("startTimeUnixNano")
        public long startTimeUnixNano;
        @JsonProperty("endTimeUnixNano")
        public long endTimeUnixNano;
        @JsonProperty("attributes")
        public List<Attribute> attributes;
        @JsonProperty("events")
        public List<Event> events;
        @JsonProperty("status")
        public Status status;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Event {
        @JsonProperty("timeUnixNano")
        public String timeUnixNano;
        @JsonProperty("name")
        public String name;
        @JsonProperty("attributes")
        public List<Attribute> attributes;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Status {
        @JsonProperty("code")
        public int code = 0;
        @JsonProperty("message")
        public String message;
    }
}