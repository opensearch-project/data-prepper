/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
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
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "date", pluginType = Processor.class, pluginConfigurationType = DateProcessorConfig.class)
public class DateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(DateProcessor.class);

    private final DateProcessorConfig dateProcessorConfig;

    private String keyToParse;
    private List<DateTimeFormatter> dateTimeFormatters;


    @DataPrepperPluginConstructor
    protected DateProcessor(PluginMetrics pluginMetrics, final DateProcessorConfig dateProcessorConfig) {
        super(pluginMetrics);
        this.dateProcessorConfig = dateProcessorConfig;

        if (dateProcessorConfig.getFromTimeReceived() && !dateProcessorConfig.getMatch().isEmpty()) {
            throw new IllegalArgumentException("from_time_received and match are mutually exclusive options.");
        }

        if (!dateProcessorConfig.getMatch().isEmpty()) {
            extractKeyAndFormatters(dateProcessorConfig.getMatch());
        }

    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            String sourceTimestamp;
            ZonedDateTime zonedDateTime = null;

            if (dateProcessorConfig.getFromTimeReceived()) {
                Instant timeReceived = record.getData().getMetadata().getTimeReceived();
                zonedDateTime = getZonedDateTimeFromInstant(timeReceived);
            }
            else {
                sourceTimestamp = record.getData().get(keyToParse, String.class);
                for (DateTimeFormatter formatter : dateTimeFormatters) {
                    zonedDateTime = getZonedDateTime(sourceTimestamp, formatter);
                }
                if (zonedDateTime == null) {
                    LOG.debug("Unable to parse {} with provided patterns.", sourceTimestamp);
                }
            }

            record.getData().put(dateProcessorConfig.getDestination(), zonedDateTime);

        }
        return records;
    }

    private ZonedDateTime getZonedDateTimeFromInstant(Instant timeReceived) {
        // define output format
        return timeReceived.atZone(ZoneId.systemDefault());
    }

    private void extractKeyAndFormatters(Map<String, List<String>> match) {
        if (match.size() > 1)
            throw new IllegalArgumentException("Only 1 key is supported.");
        for (final Map.Entry<String, List<String>> entry : dateProcessorConfig.getMatch().entrySet()) {
            keyToParse = entry.getKey();
            dateTimeFormatters = (entry.getValue().stream().map(this::getFormatter).collect(Collectors.toList()));
        }
        if (dateTimeFormatters.isEmpty())
            throw new IllegalArgumentException("At least 1 pattern is required.");
    }


    private DateTimeFormatter getFormatter(String pattern) {
        return new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.YEAR, LocalDate.now().getYear())
                .toFormatter(getLocale(dateProcessorConfig.getLocale()))
                .withZone(getZoneId(dateProcessorConfig.getTimezone()));
    }

    private Locale getLocale(String locale) {
        return Locale.forLanguageTag(locale);
    }

    private ZoneId getZoneId(String timezone) {
        return ZoneId.of(timezone);
    }

    private ZonedDateTime getZonedDateTime(String timestamp, DateTimeFormatter dateTimeFormatter) {
        try {
            return ZonedDateTime.parse(timestamp, dateTimeFormatter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ZonedDateTime.now();
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
