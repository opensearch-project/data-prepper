/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.log.JacksonLog;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
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

@DataPrepperPlugin(name = "csv", pluginType = Codec.class, pluginConfigurationType = CSVCodecConfig.class)
public class CSVCodec implements Codec {
    private final CSVCodecConfig config;
    private static final Logger LOG = LoggerFactory.getLogger(CSVCodec.class);

    @DataPrepperPluginConstructor
    public CSVCodec(final CSVCodecConfig config) {
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
            // autodetect header from the first line of csv file
            schema = createAutodetectCsvSchema();
        }
        else {
            // construct a header from the pipeline config or autogenerate it
            final int defaultBufferSize = 8192; // number of chars before mark is ignored (this is the buffer size, so large header files
            // can be read since more buffers will be allocated.)
            reader.mark(defaultBufferSize); // getting number of columns of first line will consume the line, so mark initial location

            final int firstLineSize = getSizeOfFirstLine(reader.readLine());
            reader.reset(); // move reader pointer back to beginning of file in order to parse first line
            schema = createCsvSchemaFromConfig(firstLineSize);
        }

        MappingIterator<Map<String, String>> parsingIterator = mapper.readerFor(Map.class).with(schema).readValues(reader);
        try {
            while (parsingIterator.hasNextValue()) {
                readCSVLine(parsingIterator, eventConsumer);
            }
        } catch (Exception jsonExceptionOnHasNextLine) {
            LOG.error("An Exception occurred while determining if file has next line ", jsonExceptionOnHasNextLine);
        }
    }

    private void readCSVLine(final MappingIterator<Map<String, String>> parsingIterator, final Consumer<Record<Event>> eventConsumer) {
        try {
            final Map<String, String> parsedLine = parsingIterator.nextValue();

            final Event event = JacksonLog.builder()
                    .withData(parsedLine)
                    .build();
            eventConsumer.accept(new Record<>(event));
        } catch (final CsvReadException csvException) {
            LOG.error("Invalid CSV row, skipping this line. Consider using the CSV Processor if there might be inconsistencies " +
                    "in the number of columns because it is more flexible. Error ", csvException);
        } catch (JsonParseException jsonException) {
            LOG.error("A JsonParseException occurred while reading a row of the CSV file, skipping line. Error ", jsonException);
        } catch (final Exception e) {
            LOG.error("An Exception occurred while reading a row of the CSV file. Error ", e);
        }
    }

    private int getSizeOfFirstLine(final String firstLine) {
        final CsvMapper firstLineMapper = new CsvMapper();
        firstLineMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows firstLineMapper to read with empty schema
        char delimiterAsChar = this.config.getDelimiter().charAt(0);
        char quoteCharAsChar = this.config.getQuoteCharacter().charAt(0);

        final CsvSchema getFirstLineLengthSchema = CsvSchema.emptySchema().withColumnSeparator(delimiterAsChar).
                withQuoteChar(quoteCharAsChar);

        try (final MappingIterator<List<String>> firstLineIterator = firstLineMapper.readerFor(List.class).with(getFirstLineLengthSchema)
                .readValues(firstLine)) {
            final List<String> parsedFirstLine = firstLineIterator.nextValue();

            return parsedFirstLine.size();
        } catch (final IOException e) {
            LOG.error("An exception occurred while reading first line", e);
            return 0;
        }
    }

    private CsvSchema createCsvSchemaFromConfig(final int firstLineSize) {
        final List<String> userSpecifiedHeader = config.getHeader();
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

    private String generateColumnHeader(final int colNumber) {
        final int displayColNumber = colNumber + 1; // auto generated column name indices start from 1 (not 0)
        return "column" + displayColNumber;
    }

    private CsvMapper createCsvMapper() {
        final CsvMapper mapper = new CsvMapper();
        return mapper;
    }

    private CsvSchema createAutodetectCsvSchema() {
        final char delimiterAsChar = config.getDelimiter().charAt(0); // safe due to config input validations
        final char quoteCharAsChar = config.getQuoteCharacter().charAt(0); // safe due to config input validations
        final CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(delimiterAsChar).withQuoteChar(quoteCharAsChar).withHeader();
        return schema;
    }
}
