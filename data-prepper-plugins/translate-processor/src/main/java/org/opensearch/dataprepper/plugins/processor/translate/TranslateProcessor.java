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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Objects;
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

    @DataPrepperPluginConstructor
    public TranslateProcessor(PluginMetrics pluginMetrics, final TranslateProcessorConfig translateProcessorConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.translateProcessorConfig = translateProcessorConfig;
        this.expressionEvaluator = expressionEvaluator;
        individualMappings = new HashMap<>();
        rangeMappings = new LinkedHashMap<>();
        if(this.translateProcessorConfig.getRegexParameterConfiguration()!=null) {
            patternMappings = translateProcessorConfig.getRegexParameterConfiguration().getPatterns();
        }
        else{
            patternMappings = Collections.emptyMap();
        }

        processMapField(translateProcessorConfig.getMap());
        parseFile(translateProcessorConfig.getFilePath());
        checkOverlappingKeys();
    }

    private void processMapField(Map<String, String> map){
        if(Objects.nonNull(map)) {
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
            }
            else {
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

    private void addIndividualMapping(String key, String value){
        if(individualMappings.containsKey(key)){
            String exceptionMsg = "map option contains duplicate entries of "+key;
            throw new InvalidPluginConfigurationException(exceptionMsg);
        }
        else{
            individualMappings.put(key.strip(), value);
        }
    }

    private boolean isRangeOverlapping(Range<Float> rangeEntry){
        for(Range<Float> range : rangeMappings.keySet()){
            if(range.isOverlappedBy(rangeEntry)){
                return true;
            }
        }
        return false;
    }

    private void checkOverlappingKeys(){
        for(String individualKey : individualMappings.keySet()){
            if(NumberUtils.isParsable(individualKey)){
                Float floatKey = Float.parseFloat(individualKey);
                Range<Float> range = Range.between(floatKey, floatKey);
                if(isRangeOverlapping(range)){
                    String exceptionMsg = "map option contains key "+individualKey+" that overlaps with other range entries";
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
        //todo
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (Objects.nonNull(translateProcessorConfig.getMapWhen()) && !expressionEvaluator.evaluateConditional(translateProcessorConfig.getMapWhen(), recordEvent)) {
                continue;
            }
            try {
                String matchKey = record.getData().get(translateProcessorConfig.getSource(), String.class);
                if(matchesIndividualEntry(record, matchKey) || matchesRangeEntry(record, matchKey) || matchesPatternEntry(record, matchKey)){
                    continue;
                }
                else{

                    // todo : add default, increment metrics, and/or add_tags

                }
            } catch (Exception ex){
                LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]",
                        translateProcessorConfig.getSource(), record.getData(), ex);
            }
        }
        return records;
    }

    public boolean matchesIndividualEntry(Record<Event> record, String matchKey){
        if(individualMappings.containsKey(matchKey)){
            record.getData().put(translateProcessorConfig.getTarget(), individualMappings.get(matchKey));
            return true;
        }
        return false;
    }

    public boolean matchesRangeEntry(Record<Event> record, String matchKey){
        if(!NumberUtils.isParsable(matchKey)){
            return false;
        }
        Float floatKey = Float.parseFloat(matchKey);
        for(Map.Entry<Range<Float>, String> rangeEntry : rangeMappings.entrySet()) {
            Range<Float> range = rangeEntry.getKey();
            if (range.contains(floatKey)) {
                record.getData().put(translateProcessorConfig.getTarget(), rangeEntry.getValue());
                return true;
            }
        }
        return false;
    }

    public boolean matchesPatternEntry(Record<Event> record, String matchKey){
        //todo
        if(!Objects.nonNull(patternMappings)){
            return false;
        }
        for(String pattern : patternMappings.keySet()){
            if(Pattern.matches(pattern, matchKey)){
                record.getData().put(translateProcessorConfig.getTarget(), patternMappings.get(pattern));
                return true;
            }
        }
        return false;
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
