package org.opensearch.dataprepper.plugins.source.source_crawler.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.Instant;

public class CustomInstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        long millis = p.getLongValue();
        return Instant.ofEpochMilli(millis);
    }
}
