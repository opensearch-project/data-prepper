/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.csvinputcodec;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link InputCodec} which parses CSV records into fields.
 */
@DataPrepperPlugin(name = "Csv", pluginType = InputCodec.class, pluginConfigurationType = CsvCodecConfig.class)
public class CsvInputCodec implements InputCodec {
    private static final Logger LOG = LoggerFactory.getLogger(CsvInputCodec.class);
    private final CsvCodecConfig config;

    @DataPrepperPluginConstructor
    public CsvInputCodec(CsvCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            parseBufferedReader(reader, eventConsumer);
        }
    }

    private void parseBufferedReader(final BufferedReader reader, final Consumer<Record<Event>> eventConsumer) throws IOException {
        final CsvMapper mapper = createCsvMapper();
        final CsvSchema schema;
        if (config.isDetectHeader()) {
            schema = createAutodetectHeaderCsvSchema();
        }
        else {
            final int numberColumnsFirstLine = getNumberOfColumnsByMarkingBeginningOfInputStreamAndResettingReaderAfter(reader);
            schema = createCsvSchemaFromConfig(numberColumnsFirstLine);
        }

        MappingIterator<Map<String, String>> parsingIterator = mapper.readerFor(Map.class).with(schema).readValues(reader);
        try {
            while (parsingIterator.hasNextValue()) {
                readCsvLine(parsingIterator, eventConsumer);
            }
        } catch (Exception jsonExceptionOnHasNextLine) {
            LOG.error("An Exception occurred while determining if file has next line ", jsonExceptionOnHasNextLine);
        }
    }

    private int getNumberOfColumnsByMarkingBeginningOfInputStreamAndResettingReaderAfter(BufferedReader reader) throws IOException {
        final int defaultBufferSize = 8192; // this number doesn't affect even a thousand column header — it's sufficiently large.
        reader.mark(defaultBufferSize); // calling reader.readLine() will consume the first line, so mark initial location to reset after
        final int firstLineNumberColumns = extractNumberOfColumnsFromFirstLine(reader.readLine());
        reader.reset(); // move reader pointer back to beginning of file in order to reread first line
        return firstLineNumberColumns;
    }

    private void readCsvLine(final MappingIterator<Map<String, String>> parsingIterator, final Consumer<Record<Event>> eventConsumer) {
        try {
            final Map<String, String> parsedLine = parsingIterator.nextValue();

            final Event event = JacksonLog.builder()
                    .withData(parsedLine)
                    .build();
            eventConsumer.accept(new Record<>(event));
        } catch (final CsvReadException csvException) {
            LOG.error("Invalid CSV row, skipping this line. This typically means the row has too many lines. Consider using the CSV " +
                    "Processor if there might be inconsistencies in the number of columns because it is more flexible. Error ",
                    csvException);
        } catch (JsonParseException jsonException) {
            LOG.error("A JsonParseException occurred on a row of the CSV file, skipping line. This typically means a quote character was " +
                    "not properly closed. Error ", jsonException);
        } catch (final Exception e) {
            LOG.error("An Exception occurred while reading a row of the CSV file. Error ", e);
        }
    }

    private int extractNumberOfColumnsFromFirstLine(final String firstLine) {
        if (Objects.isNull(firstLine)) {
            return 0;
        }
        int numberOfSeparators = 0;
        for (int charPointer = 0; charPointer < firstLine.length(); charPointer++) {
            if (firstLine.charAt(charPointer) == config.getDelimiter().charAt(0)) {
                numberOfSeparators++;
            }
        }
        return numberOfSeparators + 1;
    }

    private CsvSchema createCsvSchemaFromConfig(final int firstLineSize) {
        final List<String> userSpecifiedHeader = Objects.isNull(config.getHeader()) ? new ArrayList<>() : config.getHeader();
        final List<String> actualHeader = new ArrayList<>();
        final char delimiter = config.getDelimiter().charAt(0);
        final char quoteCharacter = config.getQuoteCharacter().charAt(0);
        int providedHeaderColIdx = 0;
        for (; providedHeaderColIdx < userSpecifiedHeader.size() && providedHeaderColIdx < firstLineSize; providedHeaderColIdx++) {
            actualHeader.add(userSpecifiedHeader.get(providedHeaderColIdx));
        }
        for (int remainingColIdx = providedHeaderColIdx; remainingColIdx < firstLineSize; remainingColIdx++) {
            actualHeader.add(generateColumnHeader(remainingColIdx));
        }
        CsvSchema.Builder headerBuilder = CsvSchema.builder();
        for (String columnName : actualHeader) {
            headerBuilder = headerBuilder.addColumn(columnName);
        }
        CsvSchema schema = headerBuilder.build().withColumnSeparator(delimiter).withQuoteChar(quoteCharacter);
        return schema;
    }

    private String generateColumnHeader(final int columnNumber) {
        final int displayColumnNumber = columnNumber + 1; // auto generated column name indices start from 1 (not 0)
        return "column" + displayColumnNumber;
    }

    private CsvMapper createCsvMapper() {
        final CsvMapper mapper = new CsvMapper();
        return mapper;
    }

    private CsvSchema createAutodetectHeaderCsvSchema() {
        final CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(config.getDelimiter().charAt(0))
                .withQuoteChar(config.getQuoteCharacter().charAt(0)).withHeader();
        return schema;
    }
}
