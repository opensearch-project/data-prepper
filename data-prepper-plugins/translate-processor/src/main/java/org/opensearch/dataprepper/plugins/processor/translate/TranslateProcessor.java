/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.typeconverter.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;


@DataPrepperPlugin(name = "translate", pluginType = Processor.class, pluginConfigurationType = TranslateProcessorConfig.class)
public class TranslateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(TranslateProcessor.class);
    private final ExpressionEvaluator expressionEvaluator;
    private final List<MappingsParameterConfig> mappingsConfig;

    @DataPrepperPluginConstructor
    public TranslateProcessor(PluginMetrics pluginMetrics, final TranslateProcessorConfig translateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        mappingsConfig = translateProcessorConfig.getCombinedMappingsConfigs();
        Optional.ofNullable(mappingsConfig)
                .ifPresent(configs -> configs.forEach(MappingsParameterConfig::parseMappings));
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            if(Objects.isNull(mappingsConfig)){
                continue;
            }
            final Event recordEvent = record.getData();
            for (MappingsParameterConfig mappingConfig : mappingsConfig) {
                try {
                    String iterateOn = mappingConfig.getIterateOn();
                    List<TargetsParameterConfig> targetsConfig = mappingConfig.getTargetsParameterConfigs();
                    for (TargetsParameterConfig targetConfig : targetsConfig) {
                        String translateWhen = targetConfig.getTranslateWhen();
                        Object sourceObject = mappingConfig.getSource();
                        if (Objects.nonNull(translateWhen) && !expressionEvaluator.evaluateConditional(translateWhen, recordEvent)) {
                            continue;
                        }
                        if (Objects.nonNull(iterateOn)) {
                            List<Map<String, Object>> objectsToIterate = recordEvent.get(iterateOn, List.class);
                            objectsToIterate.forEach(recordObject -> performMappings(recordObject, sourceObject, targetConfig));
                            recordEvent.put(iterateOn, objectsToIterate);
                        } else {
                            performMappings(recordEvent, sourceObject, targetConfig);
                        }
                    }
                } catch (Exception ex) {
                    LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]", mappingConfig.getSource(),
                              record.getData(), ex);
                }
            }
        }
        return records;
    }

    private String getSourceValue(Object recordObject, String sourceKey) {
        if (recordObject instanceof Map) {
            return (String) ((Map<?, ?>) recordObject).get(sourceKey);
        } else {
            return ((Event) recordObject).get(sourceKey, String.class);
        }
    }

    private Object getTargetValue(Object sourceObject, List<Object> targetValues, TargetsParameterConfig targetConfig) {
        TypeConverter converter = targetConfig.getConverter();
        if(sourceObject instanceof String) {
            return converter.convert(targetValues.get(0));
        }
        return targetValues
                .stream()
                .map(converter::convert)
                .collect(Collectors.toList());
    }

    private void performMappings(Object recordObject, Object sourceObject, TargetsParameterConfig targetConfig) {
        List<Object> targetValues = new ArrayList<>();
        List<String> sourceKeys;
        if (sourceObject instanceof List<?>) {
            sourceKeys = (ArrayList<String>) sourceObject;
        } else if (sourceObject instanceof String) {
            sourceKeys = List.of((String) sourceObject);
        } else {
            String exceptionMsg = "source option configured incorrectly. source can only be a String or list of Strings";
            throw new InvalidPluginConfigurationException(exceptionMsg);
        }
        for (String sourceKey : sourceKeys) {
            String sourceValue = getSourceValue(recordObject, sourceKey);
            if(sourceValue!=null){
                Optional<Object> targetValue = getTargetValueForSource(sourceValue, targetConfig);
                targetValue.ifPresent(targetValues::add);
            }
        }
        addTargetToRecords(sourceObject, targetValues, recordObject, targetConfig);
    }

    private Optional<Object> getTargetValueForSource(final String sourceValue, TargetsParameterConfig targetConfig) {
        Optional<Object> targetValue = Optional.empty();
        targetValue = targetValue
                .or(() -> matchesIndividualEntry(sourceValue, targetConfig))
                .or(() -> matchesRangeEntry(sourceValue, targetConfig))
                .or(() -> matchesPatternEntry(sourceValue, targetConfig))
                .or(() -> Optional.ofNullable(targetConfig.getDefaultValue()));
        return targetValue;
    }

    private Optional<Object> matchesIndividualEntry(final String sourceValue, TargetsParameterConfig targetConfig) {
        Map<String, Object> individualMappings = targetConfig.fetchIndividualMappings();
        if (individualMappings.containsKey(sourceValue)) {
            return Optional.of(individualMappings.get(sourceValue));
        }
        return Optional.empty();
    }

    private Optional<Object> matchesRangeEntry(final String sourceValue, TargetsParameterConfig targetConfig) {
        if (!NumberUtils.isParsable(sourceValue)) {
            return Optional.empty();
        }
        Float floatKey = Float.parseFloat(sourceValue);
        LinkedHashMap<Range<Float>, Object> rangeMappings = targetConfig.fetchRangeMappings();
        for (Map.Entry<Range<Float>, Object> rangeEntry : rangeMappings.entrySet()) {
            Range<Float> range = rangeEntry.getKey();
            if (range.contains(floatKey)) {
                return Optional.of(rangeEntry.getValue());
            }
        }
        return Optional.empty();
    }

    private Optional<Object> matchesPatternEntry(final String sourceValue, TargetsParameterConfig targetConfig) {
        Map<Pattern, Object> compiledPatterns = targetConfig.fetchCompiledPatterns();
        if (compiledPatterns.isEmpty()) {
            return Optional.empty();
        }
        final boolean exact = targetConfig.getRegexParameterConfiguration().getExact();
        for (Pattern pattern : compiledPatterns.keySet()) {
            Matcher matcher = pattern.matcher(sourceValue);
            if (matcher.matches() || (!exact && matcher.find())) {
                return Optional.of(compiledPatterns.get(pattern));
            }
        }
        return Optional.empty();
    }

    private void addTargetToRecords(Object sourceObject, List<Object> targetValues, Object recordObject, TargetsParameterConfig targetMappings) {
        if (targetValues.isEmpty()) {
            return;
        }
        final String targetField = targetMappings.getTarget();
        if (recordObject instanceof Map) {
            Map<String, Object> recordMap = (Map<String, Object>) recordObject;
            recordMap.put(targetField, getTargetValue(sourceObject, targetValues, targetMappings));
        } else if (recordObject instanceof Event) {
            Event event = (Event) recordObject;
            event.put(targetField, getTargetValue(sourceObject, targetValues, targetMappings));
        }
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
