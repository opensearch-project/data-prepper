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
    private static final ZoneId OUTPUT_TIMEZONE = ZoneId.systemDefault();
    static final String OUTPUT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private String keyToParse;
    private Locale sourceLocale;
    private ZoneId sourceTimezone;
    private List<DateTimeFormatter> dateTimeFormatters;
    private IllegalArgumentException parsingException;
    private final DateProcessorConfig dateProcessorConfig;

    @DataPrepperPluginConstructor
    protected DateProcessor(PluginMetrics pluginMetrics, final DateProcessorConfig dateProcessorConfig) {
        super(pluginMetrics);
        this.dateProcessorConfig = dateProcessorConfig;

        if (Boolean.TRUE.equals(dateProcessorConfig.getFromTimeReceived()) && dateProcessorConfig.getMatch() != null) {
            throw new IllegalArgumentException("from_time_received and match are mutually exclusive options.");
        }

        else if (Boolean.FALSE.equals(dateProcessorConfig.getFromTimeReceived()) && dateProcessorConfig.getMatch() != null) {
            sourceTimezone = buildZoneId(dateProcessorConfig.getTimezone());
            if (dateProcessorConfig.getLocale() == null || dateProcessorConfig.getLocale().equalsIgnoreCase("ROOT")) {
                sourceLocale = Locale.ROOT;
            }
            else {
                sourceLocale = parseLocale(dateProcessorConfig.getLocale());
            }

            if (dateProcessorConfig.getMatch().size() != 1)
                throw new IllegalArgumentException("match can have a minimum and maximum of 1 entry.");
            extractKeyAndFormatters(dateProcessorConfig.getMatch());
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            String zonedDateTime = null;

            if (Boolean.TRUE.equals(dateProcessorConfig.getFromTimeReceived()))
                zonedDateTime =  getDateTimeFromInstant(record);

            else if (keyToParse != null && !keyToParse.isEmpty())
                zonedDateTime = getDateTimeFromMatch(record);

            if (zonedDateTime != null)
                record.getData().put(dateProcessorConfig.getDestination(), zonedDateTime);
        }
        return records;
    }

    private ZoneId buildZoneId(final String timezone) {
        try {
            return ZoneId.of(timezone);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unsupported timezone provided.");
        }
    }

    private Locale parseLocale(final String localeString) {
        Locale locale = null;
        boolean isBCP47Format = localeString.contains("-");

        final String[] localeFields;
        if (isBCP47Format) {
            localeFields = localeString.split("-");
        }
        else
            localeFields = localeString.split("_");

        switch (localeFields.length) {
            case 1:
                locale = new Locale(localeFields[0]);
                break;
            case 2:
                locale = new Locale(localeFields[0], localeFields[1]);
                break;
            case 3:
                locale = new Locale(localeFields[0], localeFields[1], localeFields[2]);
                break;
            default:
                throw new IllegalArgumentException("Invalid locale format. Only language, country and variant are supported.");
        }

        if (isLocaleValid(locale))
            return locale;
        else
            throw new IllegalArgumentException("Unknown locale provided.");
    }

    private boolean isLocaleValid(Locale locale) {
        try {
            return locale.getISO3Language() != null && locale.getISO3Country() != null;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private void extractKeyAndFormatters(final Map<String, List<String>> match) {
        for (final Map.Entry<String, List<String>> entry : match.entrySet()) {
            keyToParse = entry.getKey();
            dateTimeFormatters = (entry.getValue().stream().map(this::getSourceFormatter).collect(Collectors.toList()));
        }
        if (dateTimeFormatters.isEmpty())
            throw new IllegalArgumentException("At least 1 pattern is required.");
    }

    private DateTimeFormatter getSourceFormatter(final String pattern) {
        final ZonedDateTime zonedDateTimeForDefaultValues = ZonedDateTime.now();
        return new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.YEAR, zonedDateTimeForDefaultValues.getYear())
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, zonedDateTimeForDefaultValues.getMonthValue())
                .parseDefaulting(ChronoField.DAY_OF_MONTH, zonedDateTimeForDefaultValues.getDayOfMonth())
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter(sourceLocale)
                .withZone(sourceTimezone);
    }

    private String getDateTimeFromInstant(final Record<Event> record) {
        Instant timeReceived = record.getData().getMetadata().getTimeReceived();
        return timeReceived.atZone(OUTPUT_TIMEZONE).format(getOutputFormatter());
    }

    private String getDateTimeFromMatch(final Record<Event> record) {
        String datetime = null;
        String sourceTimestamp = null;
        try {
            sourceTimestamp = record.getData().get(keyToParse, String.class);
        } catch (Exception e) {
            LOG.debug("Unable to find {} in event data.", keyToParse);
        }

        for (DateTimeFormatter formatter : dateTimeFormatters) {
            try {
                datetime = ZonedDateTime.parse(sourceTimestamp, formatter).format(getOutputFormatter().withZone(OUTPUT_TIMEZONE));
            } catch (Exception ignored) {

            }
            if (datetime != null)
                break;
        }
        if (datetime == null) {
            LOG.debug("Unable to parse {} with provided patterns", sourceTimestamp);
        }
        return datetime;
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
