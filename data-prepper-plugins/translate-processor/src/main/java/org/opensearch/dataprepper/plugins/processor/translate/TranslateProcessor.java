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
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;


@DataPrepperPlugin(name = "translate", pluginType = Processor.class, pluginConfigurationType = TranslateProcessorConfig.class)
public class TranslateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(TranslateProcessor.class);
    private final ExpressionEvaluator expressionEvaluator;
    private final TranslateProcessorConfig translateProcessorConfig;
    private final LinkedHashMap<Range<Float>, String> rangeMappings;
    private final Map<String, String> individualMappings;
    private final Map<String, String> patternMappings;
    private final Map<Pattern, String> compiledPatterns;

    @DataPrepperPluginConstructor
    public TranslateProcessor(PluginMetrics pluginMetrics, final TranslateProcessorConfig translateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.translateProcessorConfig = translateProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        individualMappings = new HashMap<>();
        rangeMappings = new LinkedHashMap<>();
        patternMappings = new HashMap<>();
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

    private void compilePatterns(Map<String, String> mappings) {
        patternMappings.putAll(mappings);
        for (String pattern : mappings.keySet()) {
            Pattern compiledPattern = Pattern.compile(pattern);
            compiledPatterns.put(compiledPattern, patternMappings.get(pattern));
        }
    }

    private void processMapField(Map<String, String> map) {
        if (Objects.nonNull(map)) {
            for (Map.Entry<String, String> mapEntry : map.entrySet()) {
                parseIndividualKeys(mapEntry);
            }
        }
    }

    private void parseIndividualKeys(Map.Entry<String, String>  mapEntry){
        String[] commaSeparatedKeys = mapEntry.getKey().split(",");
        for(String individualKey : commaSeparatedKeys){
            if(individualKey.contains("-")){
                addRangeMapping(Map.entry(individualKey, mapEntry.getValue()));
            } else {
                addIndividualMapping(individualKey, mapEntry.getValue());
            }
        }
    }

    private void addRangeMapping(Map.Entry<String, String>  mapEntry){
        String[] rangeKeys = mapEntry.getKey().split("-");
        if(rangeKeys.length!=2 || !StringUtils.isNumericSpace(rangeKeys[0]) || !StringUtils.isNumericSpace(rangeKeys[1])){
            addIndividualMapping(mapEntry.getKey(), mapEntry.getValue());
        }
        else {
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

    private void addIndividualMapping(final String key, final String value){
        if(individualMappings.containsKey(key)){
            String exceptionMsg = "map option contains duplicate entries of "+key;
            throw new InvalidPluginConfigurationException(exceptionMsg);
        }
        else{
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
                if (Objects.nonNull(translateProcessorConfig.getIterateOn())) {
                    List<Map<String, Object>> nestedObjects = recordEvent.get(translateProcessorConfig.getIterateOn(), List.class);
                    for (Map<String, Object> nextedObject : nestedObjects) {
                        performMappings(nextedObject);
                    }
                    recordEvent.put(translateProcessorConfig.getIterateOn(), nestedObjects);
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

    private String getSourceValue(Object nestedObject, String sourceKey) {
        if (nestedObject instanceof Map) {
            return (String) ((Map<?, ?>) nestedObject).get(sourceKey);
        } else {
            return ((Event) nestedObject).get(sourceKey, String.class);
        }
    }

    private Object getTargetValue(Object sourceObject, List<String> targetValues){
        if(sourceObject instanceof String){
            return targetValues.get(0);
        }
        return targetValues;
    }

    private void performMappings(Object recordObject) {
        List<String> targetValues = new ArrayList<>();
        Object sourceObject = translateProcessorConfig.getSource();
        if (sourceObject instanceof List<?>) {
            List<String> sourceKeys = (ArrayList<String>) sourceObject;
            for (String sourceKey : sourceKeys) {
                String sourceValue = getSourceValue(recordObject, sourceKey);
                populateTarget(sourceValue, targetValues);
            }
        } else if (sourceObject instanceof String) {
            String sourceKey = (String) sourceObject;
            String sourceValue = getSourceValue(recordObject, sourceKey);
            populateTarget(sourceValue, targetValues);
        }
        addTargetToRecords(sourceObject, targetValues, recordObject);
    }

    private void populateTarget(final String sourceKey, List<String> targetValues) {
        Optional<String> targetValue = Optional.empty();
        targetValue = targetValue
                .or(() -> matchesIndividualEntry(sourceKey))
                .or(() -> matchesRangeEntry(sourceKey))
                .or(() -> matchesPatternEntry(sourceKey))
                .or(() -> Optional.ofNullable(translateProcessorConfig.getDefaultValue()));
        targetValue.ifPresent(targetValues::add);
    }

    private Optional<String> matchesIndividualEntry(final String sourceKey) {
        if (individualMappings.containsKey(sourceKey)) {
            return Optional.of(individualMappings.get(sourceKey));
        }
        return Optional.empty();
    }

    private Optional<String> matchesRangeEntry(final String sourceKey) {
        if (!NumberUtils.isParsable(sourceKey)) {
            return Optional.empty();
        }
        Float floatKey = Float.parseFloat(sourceKey);
        for (Map.Entry<Range<Float>, String> rangeEntry : rangeMappings.entrySet()) {
            Range<Float> range = rangeEntry.getKey();
            if (range.contains(floatKey)) {
                return Optional.of(rangeEntry.getValue());
            }
        }
        return Optional.empty();
    }

    private Optional<String> matchesPatternEntry(final String sourceKey) {
        if (!compiledPatterns.isEmpty()) {
            for (Pattern pattern : compiledPatterns.keySet()) {
                if (pattern.matcher(sourceKey).matches()) {
                    return Optional.of(compiledPatterns.get(pattern));
                }
            }
            if (!translateProcessorConfig.getRegexParameterConfiguration().getExact()) {
                for (String pattern : patternMappings.keySet()) {
                    if (pattern.contains(sourceKey)) {
                        return Optional.of(patternMappings.get(pattern));
                    }
                }
            }
        }
        return Optional.empty();
    }

    private void addTargetToRecords(Object sourceObject, List<String> targetValues, Object recordObject) {
        String targetField = translateProcessorConfig.getTarget();
        if (!targetValues.isEmpty()) {
            if(recordObject instanceof Map){
                Map<String, Object> recordMap = (Map<String, Object>) recordObject;
                recordMap.put(targetField, getTargetValue(sourceObject, targetValues));
            }
            else if(recordObject instanceof Event){
                Event event = (Event) recordObject;
                event.put(targetField, getTargetValue(sourceObject, targetValues));
            }
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
