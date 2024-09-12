/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Processor to parse CSV data in Events.
 *
 */
@DataPrepperPlugin(name="csv", pluginType = Processor.class, pluginConfigurationType = CsvProcessorConfig.class)
public class CsvProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(CsvProcessor.class);

    static final String CSV_INVALID_EVENTS = "csvInvalidEvents";

    private final Counter csvInvalidEventsCounter;

    private final CsvProcessorConfig config;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public CsvProcessor(final PluginMetrics pluginMetrics,
                        final CsvProcessorConfig config,
                        final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.csvInvalidEventsCounter = pluginMetrics.counter(CSV_INVALID_EVENTS);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;

        if (config.getCsvWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(config.getCsvWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("csv_when value of %s is not a valid expression statement. " +
                            "See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax.", config.getCsvWhen()));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final CsvMapper mapper = createCsvMapper();
        final CsvSchema schema = createCsvSchema();

        for (final Record<Event> record : records) {

            final Event event = record.getData();

            try {

                if (config.getCsvWhen() != null && !expressionEvaluator.evaluateConditional(config.getCsvWhen(), event)) {
                    continue;
                }

                final String message = event.get(config.getSource(), String.class);

                if (Objects.isNull(message)) {
                    continue;
                }

                final boolean userDidSpecifyHeaderEventKey = Objects.nonNull(config.getColumnNamesSourceKey());
                final boolean thisEventHasHeaderSource = userDidSpecifyHeaderEventKey && event.containsKey(config.getColumnNamesSourceKey());

                final MappingIterator<List<String>> messageIterator = mapper.readerFor(List.class).with(schema).readValues(message);

                // otherwise the message is empty
                if (messageIterator.hasNextValue()) {
                    final List<String> row = messageIterator.nextValue();
                    final List<String> header = parseHeader(event, thisEventHasHeaderSource, mapper, schema);
                    putDataInEvent(event, header, row);
                }

                if (thisEventHasHeaderSource && Boolean.TRUE.equals(config.isDeleteHeader())) {
                    event.delete(config.getColumnNamesSourceKey());
                }

                if (config.isDeleteSource()) {
                    event.delete(config.getSource());
                }
            } catch (final IOException e) {
                csvInvalidEventsCounter.increment();
                LOG.error(NOISY, "An exception occurred while reading event [{}]", event, e);
            }
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }

    private CsvMapper createCsvMapper() {
        final CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY); // allows mapper to read with empty schema
        return mapper;
    }

    private CsvSchema createCsvSchema() {
        final char delimiterAsChar = config.getDelimiter().charAt(0); // safe due to config input validations
        final char quoteCharAsChar = config.getQuoteCharacter().charAt(0); // safe due to config input validations
        final CsvSchema schema = CsvSchema.emptySchema().withColumnSeparator(delimiterAsChar).withQuoteChar(quoteCharAsChar);
        return schema;
    }

    private List<String> parseHeader(final Event event, final boolean thisEventHasHeaderSource, final CsvMapper mapper,
                                     final CsvSchema schema) {
        if (thisEventHasHeaderSource) {
            return parseHeaderFromEventSourceKey(event, mapper, schema);
        }
        else if (Objects.nonNull(config.getColumnNames())) {
            return config.getColumnNames();
        }
        else {
            final List<String> emptyHeader = new ArrayList<>();
            return emptyHeader;
        }
    }

    private List<String> parseHeaderFromEventSourceKey(final Event event, final CsvMapper mapper, final CsvSchema schema) {
        try {
            final String headerUnprocessed = event.get(config.getColumnNamesSourceKey(), String.class);
            final MappingIterator<List<String>> headerIterator = mapper.readerFor(List.class).with(schema)
                    .readValues(headerUnprocessed);
            // if header is empty, behaves correctly since columns are autogenerated.
            final List<String> headerFromEventSource = headerIterator.nextValue();
            return headerFromEventSource;
        }
        catch (final IOException e) {
            LOG.debug("Auto generating header because of IOException on the header of event [{}]", event, e);
            final List<String> emptyHeader = new ArrayList<>();
            return emptyHeader;
        }
    }

    private void putDataInEvent(final Event event, final List<String> header, final List<String> data) {
        int providedHeaderColIdx = 0;
        for (; providedHeaderColIdx < header.size() && providedHeaderColIdx < data.size(); providedHeaderColIdx++) {
            event.put(header.get(providedHeaderColIdx), data.get(providedHeaderColIdx));
        }
        for (int remainingColIdx = providedHeaderColIdx; remainingColIdx < data.size(); remainingColIdx++) {
            event.put(generateColumnHeader(remainingColIdx), data.get(remainingColIdx));
        }
    }

    private String generateColumnHeader(final int colNumber) {
        final int displayColNumber = colNumber + 1; // auto generated column name indices start from 1 (not 0)
        return "column" + displayColNumber;
    }
}
