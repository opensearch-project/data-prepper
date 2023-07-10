/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;
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
    private final TranslateProcessorConfig translateProcessorConfig;
    private final LinkedHashMap<Range<Float>, Object> rangeMappings;
    private final Map<String, Object> individualMappings;
    private final Map<Pattern, Object> compiledPatterns;
    private final TypeConverter converter;

    @DataPrepperPluginConstructor
    public TranslateProcessor(PluginMetrics pluginMetrics, final TranslateProcessorConfig translateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.translateProcessorConfig = translateProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        this.converter = translateProcessorConfig.getTargetType().getTargetConverter();
        individualMappings = new HashMap<>();
        rangeMappings = new LinkedHashMap<>();
        compiledPatterns = new HashMap<>();
        if (Objects.nonNull(this.translateProcessorConfig.getRegexParameterConfiguration())) {
            compilePatterns(translateProcessorConfig
                                    .getRegexParameterConfiguration()
                                    .getPatterns());
        }
        processMapField(translateProcessorConfig.getMap());
        parseFile(translateProcessorConfig.getFilePath());
        checkOverlappingKeys();
    }

    private void compilePatterns(Map<String, Object> mappings) {
        for (String pattern : mappings.keySet()) {
            Pattern compiledPattern = Pattern.compile(pattern);
            compiledPatterns.put(compiledPattern, mappings.get(pattern));
        }
    }

    private void processMapField(Map<String, Object> map) {
        if (Objects.nonNull(map)) {
            for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
                parseIndividualKeys(mapEntry);
            }
        }
    }

    private void parseIndividualKeys(Map.Entry<String, Object>  mapEntry){
        String[] commaSeparatedKeys = mapEntry.getKey().split(",");
        for(String individualKey : commaSeparatedKeys){
            if(individualKey.contains("-")){
                addRangeMapping(Map.entry(individualKey, mapEntry.getValue()));
            } else {
                addIndividualMapping(individualKey, mapEntry.getValue());
            }
        }
    }

    private void addRangeMapping(Map.Entry<String, Object>  mapEntry){
        String[] rangeKeys = mapEntry.getKey().split("-");
        if(rangeKeys.length!=2 || !StringUtils.isNumericSpace(rangeKeys[0]) || !StringUtils.isNumericSpace(rangeKeys[1])){
            addIndividualMapping(mapEntry.getKey(), mapEntry.getValue());
        } else {
            Float lowKey = Float.parseFloat(rangeKeys[0]);
            Float highKey = Float.parseFloat(rangeKeys[1]);
            Range<Float> rangeEntry = Range.between(lowKey, highKey);
            if (isRangeOverlapping(rangeEntry)) {
                String exceptionMsg = "map option contains key "+mapEntry.getKey()+" that overlaps with other range entries";
                throw new InvalidPluginConfigurationException(exceptionMsg);
            } else {
                rangeMappings.put(Range.between(lowKey, highKey), mapEntry.getValue());
            }
        }
    }

    private void addIndividualMapping(final String key, final Object value){
        if(individualMappings.containsKey(key)){
            String exceptionMsg = "map option contains duplicate entries of "+key;
            throw new InvalidPluginConfigurationException(exceptionMsg);
        } else {
            individualMappings.put(key.strip(), value);
        }
    }

    private boolean isRangeOverlapping(Range<Float> rangeEntry) {
        for (Range<Float> range : rangeMappings.keySet()) {
            if (range.isOverlappedBy(rangeEntry)) {
                return true;
            }
        }
        return false;
    }

    private void checkOverlappingKeys() {
        for (String individualKey : individualMappings.keySet()) {
            if (NumberUtils.isParsable(individualKey)) {
                Float floatKey = Float.parseFloat(individualKey);
                Range<Float> range = Range.between(floatKey, floatKey);
                if (isRangeOverlapping(range)) {
                    String exceptionMsg = "map option contains key " + individualKey + " that overlaps with other range entries";
                    throw new InvalidPluginConfigurationException(exceptionMsg);
                }
            }
        }
    }

    private void parseFile(String filePath){
        //todo
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (Objects.nonNull(translateProcessorConfig.getTranslateWhen()) && !expressionEvaluator.evaluateConditional(translateProcessorConfig.getTranslateWhen(), recordEvent)) {
                continue;
            }
            try {
                String iterateOn = translateProcessorConfig.getIterateOn();
                if (Objects.nonNull(iterateOn)) {
                    List<Map<String, Object>> objectsToIterate = recordEvent.get(iterateOn, List.class);
                    for (Map<String, Object> recordObject : objectsToIterate) {
                        performMappings(recordObject);
                    }
                    recordEvent.put(iterateOn, objectsToIterate);
                } else {
                    performMappings(recordEvent);
                }
            } catch (Exception ex) {
                LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]", translateProcessorConfig.getSource(),
                          record.getData(), ex);
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

    private Object getTargetValue(Object sourceObject, List<Object> targetValues){
        if(sourceObject instanceof String) {
            return converter.convert(targetValues.get(0));
        }
        return targetValues.stream().map(converter::convert).collect(Collectors.toList());
    }

    private void performMappings(Object recordObject) {
        List<Object> targetValues = new ArrayList<>();
        Object sourceObject = translateProcessorConfig.getSource();
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
            Optional<Object> targetValue = getTargetValueForSource(sourceValue);
            targetValue.ifPresent(targetValues::add);
        }
        addTargetToRecords(sourceObject, targetValues, recordObject);
    }

    private Optional<Object> getTargetValueForSource(final String sourceValue) {
        Optional<Object> targetValue = Optional.empty();
        targetValue = targetValue
                .or(() -> matchesIndividualEntry(sourceValue))
                .or(() -> matchesRangeEntry(sourceValue))
                .or(() -> matchesPatternEntry(sourceValue))
                .or(() -> Optional.ofNullable(translateProcessorConfig.getDefaultValue()));
        return targetValue;
    }

    private Optional<Object> matchesIndividualEntry(final String sourceValue) {
        if (individualMappings.containsKey(sourceValue)) {
            return Optional.of(individualMappings.get(sourceValue));
        }
        return Optional.empty();
    }

    private Optional<Object> matchesRangeEntry(final String sourceValue) {
        if (!NumberUtils.isParsable(sourceValue)) {
            return Optional.empty();
        }
        Float floatKey = Float.parseFloat(sourceValue);
        for (Map.Entry<Range<Float>, Object> rangeEntry : rangeMappings.entrySet()) {
            Range<Float> range = rangeEntry.getKey();
            if (range.contains(floatKey)) {
                return Optional.of(rangeEntry.getValue());
            }
        }
        return Optional.empty();
    }

    private Optional<Object> matchesPatternEntry(final String sourceValue) {
        if (compiledPatterns.isEmpty()) {
            return Optional.empty();
        }
        final boolean exact = translateProcessorConfig.getRegexParameterConfiguration().getExact();
        for (Pattern pattern : compiledPatterns.keySet()) {
            Matcher matcher = pattern.matcher(sourceValue);
            if (matcher.matches() || (!exact && matcher.find())) {
                return Optional.of(compiledPatterns.get(pattern));
            }
        }
        return Optional.empty();
    }

    private void addTargetToRecords(Object sourceObject, List<Object> targetValues, Object recordObject) {
        if (targetValues.isEmpty()) {
            return;
        }
        final String targetField = translateProcessorConfig.getTarget();
        if (recordObject instanceof Map) {
            Map<String, Object> recordMap = (Map<String, Object>) recordObject;
            recordMap.put(targetField, getTargetValue(sourceObject, targetValues));
        } else if (recordObject instanceof Event) {
            Event event = (Event) recordObject;
            event.put(targetField, getTargetValue(sourceObject, targetValues));
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
