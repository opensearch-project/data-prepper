/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as CSV Data
 */
@DataPrepperPlugin(name = "csv", pluginType = OutputCodec.class, pluginConfigurationType = CsvOutputCodecConfig.class)
public class CsvOutputCodec implements OutputCodec {
    private final CsvOutputCodecConfig config;
    private static int OVERHEAD_BYTES = 0;
    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputCodec.class);
    private static final String CSV = "csv";
    private static final String DELIMITER = ",";
    private int headerLength = 0;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private List<String> headerList;
    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public CsvOutputCodec(final CsvOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    private byte[] getHeaderByteArray() throws IOException {
        if (config.getHeader() != null) {
            headerList = config.getHeader();
        } else if (config.getHeaderFileLocation() != null) {
            try {
                headerList = CsvHeaderParser.headerParser(config.getHeaderFileLocation());
            } catch (Exception e) {
                LOG.error("Unable to parse CSV Header, Error:{} ", e.getMessage());
                throw new IOException("Unable to parse CSV Header.");
            }
        } else if (checkS3HeaderValidity()) {
            headerList = CsvHeaderParserFromS3.parseHeader(config);
        } else {
            LOG.error("No header provided.");
            throw new IOException("No header found. Can't proceed without header.");
        }

        headerLength = headerList.size();
        return String.join(config.getDelimiter(), headerList).getBytes();
    }

    @Override
    public void start(final OutputStream outputStream, Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.codecContext = codecContext;
        writeToOutputStream(outputStream, getHeaderByteArray());
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        outputStream.close();
    }

    private byte[] getEventSerializedBytes(final Event event) throws IOException, IllegalArgumentException {
        Objects.requireNonNull(event);
        final Map<String, Object> eventMap;
        if (codecContext.getTagsTargetKey() != null) {
            eventMap = addTagsToEvent(event, codecContext.getTagsTargetKey()).toMap();
        } else {
            eventMap = event.toMap();
        }

        if (!codecContext.getExcludeKeys().isEmpty()) {
            for (final String key : codecContext.getExcludeKeys()) {
                eventMap.remove(key);
            }
        }

        for (final Map.Entry entry : eventMap.entrySet()) {
            final Object mapValue = entry.getValue();
            entry.setValue(objectMapper.writeValueAsString(mapValue));
        }

        final List<String> valueList = eventMap.entrySet().stream().map(map -> map.getValue().toString())
                .collect(Collectors.toList());
        if (headerLength != valueList.size()) {
            throw new IllegalArgumentException("CSV Row doesn't conform with header");
        }
        return valueList.stream().collect(Collectors.joining(DELIMITER)).getBytes();
    }

    @Override
    public int getEstimatedSize(Event event) throws IOException {
        try {
            return getHeaderByteArray().length + getEventSerializedBytes(event).length;
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        try {
            writeToOutputStream(outputStream, getEventSerializedBytes(event));
        } catch (IllegalArgumentException e) {
            LOG.error("CSV Row doesn't conform with the header.");
            return;
        }
    }

    private void writeToOutputStream(final OutputStream outputStream, final byte[] byteArr) throws IOException {
        outputStream.write(byteArr);
        outputStream.write(System.lineSeparator().getBytes());
    }

    @Override
    public String getExtension() {
        return CSV;
    }

    private boolean checkS3HeaderValidity() throws IOException {
        if (config.getBucketName() != null && config.getFile_key() != null && config.getRegion() != null) {
            return true;
        } else {
            LOG.error("Invalid S3 credentials, can't reach the header file.");
            throw new IOException("Can't proceed without header.");
        }
    }
}
