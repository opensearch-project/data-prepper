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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "date", pluginType = Processor.class, pluginConfigurationType = DateProcessorConfig.class)
public class DateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(DateProcessor.class);
    private static final String OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final int LENGTH_OF_EPOCH_IN_MILLIS = 13;
    private static final int LENGTH_OF_EPOCH_SECONDS = 10;

    static final String DATE_PROCESSING_MATCH_SUCCESS = "dateProcessingMatchSuccess";
    static final String DATE_PROCESSING_MATCH_FAILURE = "dateProcessingMatchFailure";

    private String keyToParse;
    private List<DateTimeFormatter> dateTimeFormatters;
    private Set<String> epochFormatters;
    private String outputFormat;
    private final DateProcessorConfig dateProcessorConfig;
    private final ExpressionEvaluator expressionEvaluator;

    private final Counter dateProcessingMatchSuccessCounter;
    private final Counter dateProcessingMatchFailureCounter;

    @DataPrepperPluginConstructor
    public DateProcessor(PluginMetrics pluginMetrics, final DateProcessorConfig dateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.dateProcessorConfig = dateProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.outputFormat = dateProcessorConfig.getOutputFormat();

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
            epochFormatters = entry.getPatterns().stream().filter(pattern -> pattern.contains("epoch")).collect(Collectors.toSet());
            dateTimeFormatters = entry.getPatterns().stream().filter(pattern -> !pattern.contains("epoch")).map(this::getSourceFormatter).collect(Collectors.toList());
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

    private Pair<String, Instant> getEpochFormatOutput(Instant time) {
        if (outputFormat.equals("epoch_second")) {
            return Pair.of(Long.toString(time.getEpochSecond()), time);
        } else if (outputFormat.equals("epoch_milli")) {
            return Pair.of(Long.toString(time.toEpochMilli()), time);
        } else { // epoch_nano. validation for valid epoch_ should be
                 // done at init time
            long nano = (long)time.getEpochSecond() * 1000_000_000 + (long) time.getNano();
            return Pair.of(Long.toString(nano), time);
        } 
    }

    private Pair<String, Instant> getFormattedDateTimeString(final String sourceTimestamp) {
        ZoneId srcZoneId = dateProcessorConfig.getSourceZoneId();
        ZoneId dstZoneId = dateProcessorConfig.getDestinationZoneId();
        Long numberValue = null;
        Instant epochTime;
        
        if (epochFormatters.size() > 0) {
            try {
                numberValue = Long.parseLong(sourceTimestamp);
            } catch (NumberFormatException e) {
                numberValue = null;
            }
        }
        if (numberValue != null) {
            int timestampLength = sourceTimestamp.length();
            if (timestampLength > LENGTH_OF_EPOCH_IN_MILLIS) {
                if (epochFormatters.contains("epoch_nano")) {
                    epochTime = Instant.ofEpochSecond(numberValue/1000_000_000, numberValue % 1000_000_000);
                } else {
                    LOG.warn("Source time value is larger than epoch pattern configured. epoch_nano is expected but not present in the patterns list");
                    return null;
                }
            } else if (timestampLength > LENGTH_OF_EPOCH_SECONDS) {
                if (epochFormatters.contains("epoch_milli")) {
                    epochTime = Instant.ofEpochMilli(numberValue);
                } else {
                    LOG.warn("Source time value is larger than epoch pattern configured. epoch_milli is expected but not present in the patterns list");
                    return null;
                }
            } else {
                epochTime = Instant.ofEpochSecond(numberValue);
            }
            // Epochs are always UTC zone
            srcZoneId = ZoneId.of("UTC");
            try {
                if (outputFormat.startsWith("epoch_")) {
                    return getEpochFormatOutput(epochTime);
                } else {
                    DateTimeFormatter outputFormatter = getOutputFormatter().withZone(dstZoneId);
                    ZonedDateTime tmp = ZonedDateTime.ofInstant(epochTime, srcZoneId);
                    return Pair.of(tmp.format(outputFormatter.withZone(dstZoneId)), tmp.toInstant());
                }
            } catch (Exception ignored) {
            }
        }

        for (DateTimeFormatter formatter : dateTimeFormatters) {
            try {
                ZonedDateTime tmp = ZonedDateTime.parse(sourceTimestamp, formatter);
                if (outputFormat.startsWith("epoch_")) {
                    return getEpochFormatOutput(tmp.toInstant());
                }
                return Pair.of(tmp.format(getOutputFormatter().withZone(dstZoneId)), tmp.toInstant());
            } catch (Exception ignored) {
            }
        }

        LOG.debug("Unable to parse {} with any of the provided patterns", sourceTimestamp);
        return null;
    }

    private DateTimeFormatter getOutputFormatter() {
        return DateTimeFormatter.ofPattern(outputFormat);
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
