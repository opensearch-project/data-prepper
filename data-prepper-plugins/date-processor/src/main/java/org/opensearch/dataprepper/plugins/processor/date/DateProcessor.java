/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.date;

import io.micrometer.core.instrument.Counter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "date", pluginType = Processor.class, pluginConfigurationType = DateProcessorConfig.class)
public class DateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(DateProcessor.class);
    private static final String OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    static final String DATE_PROCESSING_MATCH_SUCCESS = "dateProcessingMatchSuccess";
    static final String DATE_PROCESSING_MATCH_FAILURE = "dateProcessingMatchFailure";

    private String keyToParse;
    private List<DateTimeFormatter> dateTimeFormatters;
    private final DateProcessorConfig dateProcessorConfig;
    private final ExpressionEvaluator expressionEvaluator;

    private final Counter dateProcessingMatchSuccessCounter;
    private final Counter dateProcessingMatchFailureCounter;

    @DataPrepperPluginConstructor
    public DateProcessor(PluginMetrics pluginMetrics, final DateProcessorConfig dateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.dateProcessorConfig = dateProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;

        dateProcessingMatchSuccessCounter = pluginMetrics.counter(DATE_PROCESSING_MATCH_SUCCESS);
        dateProcessingMatchFailureCounter = pluginMetrics.counter(DATE_PROCESSING_MATCH_FAILURE);

        if (dateProcessorConfig.getMatch() != null)
            extractKeyAndFormatters();
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {

            try {
                if (Objects.nonNull(dateProcessorConfig.getDateWhen()) && !expressionEvaluator.evaluateConditional(dateProcessorConfig.getDateWhen(), record.getData())) {
                    continue;
                }

                String zonedDateTime = null;

                if (Boolean.TRUE.equals(dateProcessorConfig.getFromTimeReceived())) {
                    zonedDateTime =  getDateTimeFromTimeReceived(record);

                } else if (keyToParse != null && !keyToParse.isEmpty()) {
                    Pair<String, Instant> result = getDateTimeFromMatch(record);
                    if (result != null) {
                        zonedDateTime = result.getLeft();
                        Instant timeStamp = result.getRight();
                        if (dateProcessorConfig.getToOriginationMetadata()) {
                            Event event = (Event)record.getData();
                            event.getMetadata().setExternalOriginationTime(timeStamp);
                            event.getEventHandle().setExternalOriginationTime(timeStamp);
                        }
                    }
                    populateDateProcessorMetrics(zonedDateTime);
                }

                if (zonedDateTime != null) {
                    record.getData().put(dateProcessorConfig.getDestination(), zonedDateTime);
                }
            } catch (final Exception e) {
                LOG.error("An exception occurred while attempting to process Event: ", e);
            }
        }
        return records;
    }

    private void populateDateProcessorMetrics(final String zonedDateTime) {
        if (zonedDateTime != null)
            dateProcessingMatchSuccessCounter.increment();
        else
            dateProcessingMatchFailureCounter.increment();
    }

    private void extractKeyAndFormatters() {
        for (DateProcessorConfig.DateMatch entry: dateProcessorConfig.getMatch()) {
            keyToParse = entry.getKey();
            dateTimeFormatters = entry.getPatterns().stream().map(this::getSourceFormatter).collect(Collectors.toList());
        }
    }

    private DateTimeFormatter getSourceFormatter(final String pattern) {
        final LocalDate localDateForDefaultValues = LocalDate.now(dateProcessorConfig.getSourceZoneId());

        final DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, localDateForDefaultValues.getMonthValue())
                .parseDefaulting(ChronoField.DAY_OF_MONTH, localDateForDefaultValues.getDayOfMonth())
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);

        if (!(pattern.contains("y") || pattern.contains("u")))
            dateTimeFormatterBuilder.parseDefaulting(ChronoField.YEAR_OF_ERA, localDateForDefaultValues.getYear());

        return dateTimeFormatterBuilder
                .toFormatter(dateProcessorConfig.getSourceLocale())
                .withZone(dateProcessorConfig.getSourceZoneId());
    }

    private String getDateTimeFromTimeReceived(final Record<Event> record) {
        final Instant timeReceived = record.getData().getMetadata().getTimeReceived();
        return timeReceived.atZone(dateProcessorConfig.getDestinationZoneId()).format(getOutputFormatter());
    }

    private Pair<String, Instant> getDateTimeFromMatch(final Record<Event> record) {
        final String sourceTimestamp = getSourceTimestamp(record);
        if (sourceTimestamp == null)
            return null;

        return getFormattedDateTimeString(sourceTimestamp);
    }

    private String getSourceTimestamp(final Record<Event> record) {
        try {
            return record.getData().get(keyToParse, String.class);
        } catch (Exception e) {
            LOG.debug("Unable to find {} in event data.", keyToParse);
            return null;
        }
    }

    private Pair<String, Instant> getFormattedDateTimeString(final String sourceTimestamp) {
        for (DateTimeFormatter formatter : dateTimeFormatters) {
            try {
                ZonedDateTime tmp = ZonedDateTime.parse(sourceTimestamp, formatter);
                return Pair.of(tmp.format(getOutputFormatter().withZone(dateProcessorConfig.getDestinationZoneId())), tmp.toInstant());
            } catch (Exception ignored) {
            }
        }

        LOG.debug("Unable to parse {} with any of the provided patterns", sourceTimestamp);
        return null;
    }

    private DateTimeFormatter getOutputFormatter() {
        return DateTimeFormatter.ofPattern(OUTPUT_FORMAT);
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
}
