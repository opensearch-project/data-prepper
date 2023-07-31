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
import org.opensearch.dataprepper.model.event.JacksonEvent;
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
    private final JacksonEvent.Builder eventBuilder= JacksonEvent.builder();
    private final JsonExtractor jsonExtractor = new JsonExtractor();

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
                    List<TargetsParameterConfig> targetsConfig = mappingConfig.getTargetsParameterConfigs();
                    for (TargetsParameterConfig targetConfig : targetsConfig) {
                        Object sourceObject = mappingConfig.getSource();
                        translateSource(sourceObject, recordEvent, targetConfig);
                    }
                } catch (Exception ex) {
                    LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]", mappingConfig.getSource(),
                              record.getData(), ex);
                }
            }
        }
        return records;
    }

    private List<String> getSourceKeys(Object sourceObject){
        List<String> sourceKeys;
        if (sourceObject instanceof List<?>) {
            sourceKeys = (ArrayList<String>) sourceObject;
        } else if (sourceObject instanceof String) {
            sourceKeys = List.of((String) sourceObject);
        } else {
            String exceptionMsg = "source option configured incorrectly. source can only be a String or list of Strings";
            throw new InvalidPluginConfigurationException(exceptionMsg);
        }
        return sourceKeys;
    }

    private void translateSource(Object sourceObject, Event recordEvent, TargetsParameterConfig targetConfig) {
        Map<String, Object> recordObject =  recordEvent.toMap();
        List<String> sourceKeysPaths = getSourceKeys(sourceObject);
        if(Objects.isNull(recordObject) || sourceKeysPaths.isEmpty()){
            return;
        }

        List<String> sourceKeys = new ArrayList<>();
        for(String sourceKeyPath: sourceKeysPaths){
            sourceKeys.add(jsonExtractor.getLeafField(sourceKeyPath));
        }

        String commonPath = jsonExtractor.getParentPath(sourceKeysPaths.get(0));
        if(commonPath.isEmpty()) {
            performMappings(recordEvent, sourceKeys, sourceObject, targetConfig);
            return;
        }

        String rootField = jsonExtractor.getRootField(commonPath);
        if(!recordObject.containsKey(rootField)){
            return;
        }

        List<Object> targetObjects = jsonExtractor.getObjectFromPath(commonPath, recordObject);
        if(!targetObjects.isEmpty()) {
            targetObjects.forEach(targetObj -> performMappings(targetObj, sourceKeys, sourceObject, targetConfig));
            recordEvent.put(rootField, recordObject.get(rootField));
        }
    }

    private String getSourceValue(Object recordObject, String sourceKey) {
        Optional<Object> sourceValue;
        if (recordObject instanceof Map) {
            sourceValue = Optional.ofNullable(((Map<?, ?>) recordObject).get(sourceKey));
        } else {
            sourceValue = Optional.ofNullable(((Event) recordObject).get(sourceKey, String.class));
        }
        return sourceValue.map(Object::toString).orElse(null);
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

    private void performMappings(Object recordObject, List<String> sourceKeys, Object sourceObject, TargetsParameterConfig targetConfig) {
        if (Objects.isNull(recordObject) ||
            Objects.isNull(sourceObject) ||
            Objects.isNull(targetConfig) ||
            sourceKeys.isEmpty()) {
            return;
        }
        String translateWhen = targetConfig.getTranslateWhen();
        if(!isExpressionValid(translateWhen, recordObject)){
            return;
        }
        List<Object> targetValues = new ArrayList<>();
        for (String sourceKey : sourceKeys) {
            String sourceValue = getSourceValue(recordObject, sourceKey);
            if(sourceValue!=null){
                Optional<Object> targetValue = getTargetValueForSource(sourceValue, targetConfig);
                targetValue.ifPresent(targetValues::add);
            }
        }
        addTargetToRecords(sourceObject, targetValues, recordObject, targetConfig);
    }

    private boolean isExpressionValid(String translateWhen, Object recordObject){
        Event recordEvent;
        if (recordObject instanceof Map) {
            recordEvent = eventBuilder.withData(recordObject).withEventType("event").build();
        } else {
            recordEvent = (Event)recordObject;
        }
        return (translateWhen == null) || expressionEvaluator.evaluateConditional(translateWhen, recordEvent);
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
            if (matcher.matches()) {
                return Optional.of(compiledPatterns.get(pattern));
            }
            if(!exact && matcher.find()) {
                String targetValue = (String)compiledPatterns.get(pattern);
                return Optional.of(matcher.replaceAll(targetValue));
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
