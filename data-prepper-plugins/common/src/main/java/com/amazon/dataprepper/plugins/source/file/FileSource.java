/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.file;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@DataPrepperPlugin(name = "file", pluginType = Source.class, pluginConfigurationType = FileSourceConfig.class)
public class FileSource implements Source<Record<Event>> {

    static final String MESSAGE_KEY = "message";
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final FileSourceConfig fileSourceConfig;

    private boolean isStopRequested;

    @DataPrepperPluginConstructor
    public FileSource(final FileSourceConfig fileSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        Objects.requireNonNull(fileSourceConfig.getFilePathToRead(), "File path is required");
        if (!Files.exists(Paths.get(fileSourceConfig.getFilePathToRead()))) {
            throw new IllegalArgumentException("File path does not exist.");
        }
        Objects.requireNonNull(fileSourceConfig.getFormat(), "Invalid file format. Options are [json] and [plain]");
        this.fileSourceConfig = fileSourceConfig;
        this.isStopRequested = false;
    }


    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        checkNotNull(buffer, "Buffer cannot be null for file source to start");
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileSourceConfig.getFilePathToRead()), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null && !isStopRequested) {
                buffer.write(getEventRecordFromLine(line), fileSourceConfig.getWriteTimeout());
            }
        } catch (IOException | TimeoutException ex) {
            LOG.error("Error processing the input file path [{}]", fileSourceConfig.getFilePathToRead(), ex);
            throw new RuntimeException(format("Error processing the input file %s",
                    fileSourceConfig.getFilePathToRead()), ex);
        }
    }

    @Override
    public void stop() {
        isStopRequested = true;
    }

    private Record<Event> getEventRecordFromLine(final String line) {
        Map<String, Object> structuredLine = new HashMap<>();

        switch(fileSourceConfig.getFormat()) {
            case JSON:
                structuredLine = parseJson(line);
                break;
            case PLAIN:
                structuredLine.put(MESSAGE_KEY, line);
                break;
            default:
                LOG.error("The file source type is [{}]. It must be \"json\" or \"plain\"", fileSourceConfig.getType().toString());
        }

        return new Record<>(JacksonEvent
                .builder()
                .withEventType(fileSourceConfig.getType())
                .withData(structuredLine)
                .build());
    }

    private Map<String, Object> parseJson(final String jsonString) {
        try {
            return OBJECT_MAPPER.readValue(jsonString, MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            LOG.error("Unable to parse json data [{}], assuming plain text", jsonString, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put(MESSAGE_KEY, jsonString);
            return plainMap;
        }
    }
}