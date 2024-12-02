package org.opensearch.dataprepper.plugins.lambda.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.record.RecordMetadata;
import org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LambdaTestSetupUtil {

    private static final Logger log = LoggerFactory.getLogger(LambdaTestSetupUtil.class);

    public static ObjectMapper getObjectMapper() {
        return new ObjectMapper(
                new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)).registerModule(
                new JavaTimeModule());
    }

    private static InputStream getResourceAsStream(String resourceName) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resourceName);
        if (inputStream == null) {
            inputStream = LambdaTestSetupUtil.class.getResourceAsStream("/" + resourceName);
        }
        return inputStream;
    }

    public static LambdaProcessorConfig createLambdaConfigurationFromYaml(String fileName) {
        ObjectMapper objectMapper = getObjectMapper();
        try (InputStream inputStream = getResourceAsStream(fileName)) {
            return objectMapper.readValue(inputStream, LambdaProcessorConfig.class);
        } catch (IOException ex) {
            log.error("Failed to parse pipeline Yaml", ex);
            throw new RuntimeException(ex);
        }
    }

    public static Record<Event> getSampleRecord() {
        return new Record<>(getSampleEvent(), RecordMetadata.defaultMetadata());
    }

    public static Event getSampleEvent() {
        return JacksonEvent.fromMessage(UUID.randomUUID().toString());
    }

    public static Event getSampleEventWithAttributes(String key, String value) {
        return JacksonEvent.fromEvent(JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString())
                .withTimeReceived(Instant.now())
                .withEventMetadataAttributes(Map.of(key, value))
                .build());
    }

    public static List<Record<Event>> getSampleEventRecords(int count) {
        List<Record<Event>> originalRecords = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            originalRecords.add(getSampleRecord());
        }
        return originalRecords;
    }

    public static List<Event> getSampleParsedEvents(int count) {
        List<Event> originalRecords = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            originalRecords.add(getSampleEvent());
        }
        return originalRecords;
    }

}
