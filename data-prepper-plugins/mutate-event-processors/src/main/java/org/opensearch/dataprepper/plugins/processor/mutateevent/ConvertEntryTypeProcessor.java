/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.typeconverter.ConverterArguments;
import org.opensearch.dataprepper.typeconverter.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "convert_type", deprecatedName = "convert_entry_type", pluginType = Processor.class, pluginConfigurationType = ConvertEntryTypeProcessorConfig.class)
public class ConvertEntryTypeProcessor  extends AbstractProcessor<Record<Event>, Record<Event>> {
    static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    private static final Logger LOG = LoggerFactory.getLogger(ConvertEntryTypeProcessor.class);
    private final List<String> convertEntryKeys;
    private final TypeConverter<?> converter;
    private final String convertWhen;
    private final List<String> nullValues;
    private final String type;
    private final List<String> tagsOnFailure;

    private final String iterateOn;
    private int scale = 0;

    private final ExpressionEvaluator expressionEvaluator;
    private final ConverterArguments converterArguments;
    private List<DateTimeFormatter> coerceDateTimeFormatters;

    private final ConvertEntryTypeProcessorConfig.CoerceStringsConfig coerceStrings;
    @DataPrepperPluginConstructor
    public ConvertEntryTypeProcessor(final PluginMetrics pluginMetrics,
                                     final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig,
                                     final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.converterArguments = convertEntryTypeProcessorConfig;
        this.convertEntryKeys = getKeysToConvert(convertEntryTypeProcessorConfig);
        TargetType targetType = convertEntryTypeProcessorConfig.getType();
        if (targetType != null) {
            this.type = targetType.name();
            this.converter = targetType.getTargetConverter();
        } else {
            this.type = null;
            this.converter = null;
        }
        this.scale = convertEntryTypeProcessorConfig.getScale();
        this.convertWhen = convertEntryTypeProcessorConfig.getConvertWhen();
        this.nullValues = convertEntryTypeProcessorConfig.getNullValues()
                .orElse(List.of());
        this.expressionEvaluator = expressionEvaluator;
        this.tagsOnFailure = convertEntryTypeProcessorConfig.getTagsOnFailure();
        this.iterateOn = convertEntryTypeProcessorConfig.getIterateOn();
        this.coerceStrings = convertEntryTypeProcessorConfig.getCoerceStrings();
        if (coerceStrings != null) {
            this.coerceDateTimeFormatters = coerceStrings.getCoerceStringTimeFormats().stream().map(this::getSourceFormatter).collect(Collectors.toList());
        }

        if (convertWhen != null
                && !expressionEvaluator.isValidExpressionStatement(convertWhen)) {
            throw new InvalidPluginConfigurationException(
                    String.format("convert_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", convertWhen));
        }
    }

    private DateTimeFormatter getSourceFormatter(final String pattern) {
        final LocalDate localDateForDefaultValues = LocalDate.now(DEFAULT_ZONE_ID);

        final DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, localDateForDefaultValues.getMonthValue())
                .parseDefaulting(ChronoField.DAY_OF_MONTH, localDateForDefaultValues.getDayOfMonth())
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);

        if(!pattern.contains("a") && !pattern.contains("k"))
            dateTimeFormatterBuilder.parseDefaulting(ChronoField.HOUR_OF_DAY, 0);

        if (!(pattern.contains("y") || pattern.contains("u")))
            dateTimeFormatterBuilder.parseDefaulting(ChronoField.YEAR_OF_ERA, localDateForDefaultValues.getYear());
        return dateTimeFormatterBuilder
                .toFormatter(Locale.getDefault())
                .withZone(DEFAULT_ZONE_ID);
    }

    private void doAutoConversion(Event event, Map<String, Object> map, final String keyPrefix) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object result = null;
            try {
                result = autoConvert(event, entry.getValue(), keyPrefix+entry.getKey()+"/");
                if (result != null) {
                    event.put(keyPrefix+entry.getKey(), result);
                }
            } catch (Exception ignored) {}
        }
    }

    private Object autoConvert(Event event, Object objValue, final String keyPrefix) {
        if (objValue instanceof String) {
            String str = (String)objValue;
            if (str.isEmpty())
                return null;
            String lstr = str.toLowerCase();
            Character firstChar = str.charAt(0);
            if (lstr.equals("true") || lstr.equals("false")) {
                return Boolean.parseBoolean(lstr);
            } else if (str.contains(":")) {
                for (DateTimeFormatter formatter : coerceDateTimeFormatters) {
                    try {
                        ZonedDateTime tmp = ZonedDateTime.parse(str, formatter);
                        long r = (long)tmp.toInstant().toEpochMilli();
                        return r;
                    } catch (Exception ignored) {
                    }
                }
                return null;
            } else if (lstr.contains(".") || lstr.contains("e")) {
                Double d = Double.parseDouble(lstr);
                return d;
            } else if (Character.isDigit(firstChar) || firstChar == '-' || firstChar == '+') {
                Long l = Long.parseLong(str);
                if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                    return (Integer)l.intValue();
                }
                return l;
            }

        } else if (objValue instanceof Map) {
            doAutoConversion(event, (Map<String, Object>)objValue, keyPrefix);
        } else if (objValue instanceof List) {
            List<Object> listValue = (List<Object>)objValue;
            for (int i = 0; i < listValue.size(); i++) {
                Object result = autoConvert(event, listValue.get(i), keyPrefix+Integer.toString(i)+"/");
                if (result != null) {
                    event.put(keyPrefix+Integer.toString(i), result);
                }
            }
        }
        return null;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {

                if (Objects.nonNull(convertWhen) && !expressionEvaluator.evaluateConditional(convertWhen, recordEvent)) {
                    continue;
                }

                if (coerceStrings != null) {
                    doAutoConversion(recordEvent, recordEvent.toMap(), "");
                }

                if (convertEntryKeys == null || convertEntryKeys.isEmpty()) {
                    continue;
                }
                for (final String key : convertEntryKeys) {
                    if (iterateOn != null) {
                        handleWithIterateOn(recordEvent, key);
                    } else {
                        Object keyVal = recordEvent.get(key, Object.class);
                        if (keyVal != null) {
                            if (!nullValues.contains(keyVal.toString())) {
                                try {
                                    handleWithoutIterateOn(keyVal, recordEvent, key);
                                } catch (final RuntimeException e) {
                                    LOG.error(EVENT, "Unable to convert key: {} with value: {} to {}", key, keyVal, type, e);
                                    recordEvent.getMetadata().addTags(tagsOnFailure);
                                }
                            } else {
                                recordEvent.delete(key);
                            }
                        }
                    }
                }
            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .addArgument(recordEvent)
                        .setCause(e)
                        .log();
                recordEvent.getMetadata().addTags(tagsOnFailure);
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

    private void handleWithoutIterateOn(final Object keyVal,
                                        final Event recordEvent,
                                        final String key) {
        if (keyVal instanceof List || keyVal.getClass().isArray()) {
            Stream<Object> inputStream;
            if (keyVal.getClass().isArray()) {
                inputStream = Arrays.stream((Object[])keyVal);
            } else {
                inputStream = ((List<Object>)keyVal).stream();
            }
            List<?> replacementList = inputStream.map(i -> converter.convert(i, converterArguments)).collect(Collectors.toList());
            recordEvent.put(key, replacementList);
        } else {
            recordEvent.put(key, converter.convert(keyVal, converterArguments));
        }
    }

    private void handleWithIterateOn(final Event recordEvent,
                                     final String key) {

        final List<Map<String, Object>> iterateOnList;
        try {
            iterateOnList = recordEvent.get(iterateOn, List.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(
                    String.format("The value of '%s' must be a List of Map<String, Object>, but was incompatible: %s",
                            iterateOn, e.getMessage()), e);
        }
        if (iterateOnList != null) {
            int listIndex = 0;
            for (final Map<String, Object> item : iterateOnList) {
                Object value = null;
                try {
                    value = item.get(key);
                    if (value != null) {
                        item.put(key, converter.convert(value, converterArguments));
                    }
                } catch (final RuntimeException e) {
                    LOG.error(EVENT, "Unable to convert element {} with key: {} with value: {} to {}", listIndex, key, value, type, e);
                }

                listIndex++;
            }

            recordEvent.put(iterateOn, iterateOnList);
        }
    }

    private List<String> getKeysToConvert(final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig) {
        final ConvertEntryTypeProcessorConfig.CoerceStringsConfig coerceStrings = convertEntryTypeProcessorConfig.getCoerceStrings();
        final String key = convertEntryTypeProcessorConfig.getKey();
        final List<String> keys = convertEntryTypeProcessorConfig.getKeys();
        if (key == null && keys == null && coerceStrings == null) {
            throw new IllegalArgumentException("key, keys, and coerceStrings all cannot be null. One must be provided.");
        }
        if (key != null && keys != null) {
            throw new IllegalArgumentException("key and keys cannot both be defined.");
        }
        if (key != null) {
            if (key.isEmpty()) {
                throw new IllegalArgumentException("key cannot be empty.");
            } else {
                return Collections.singletonList(key);
            }
        }
        return keys;
    }
}


