

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mapvalues;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;


@DataPrepperPlugin(name = "map_values", pluginType = Processor.class, pluginConfigurationType = MapValuesConfig.class)
public class MapValues extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(MapValues.class);
    private final ExpressionEvaluator expressionEvaluator;
    private final MapValuesConfig mapValuesConfig;
    private final LinkedHashMap<Range<Float>, String> rangeMappings;
    private final Map<String, String> individualMappings;
    private final HashMap<String, String> patternMappings;

    @DataPrepperPluginConstructor
    public MapValues(PluginMetrics pluginMetrics, final MapValuesConfig mapValuesConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.mapValuesConfig = mapValuesConfig;
        this.expressionEvaluator = expressionEvaluator;
        individualMappings = new HashMap<>();
        rangeMappings = new LinkedHashMap<>();
        patternMappings = this.mapValuesConfig.getRegexParameterConfiguration().getPatterns();
        processMapField(mapValuesConfig.getMap());
        parseFile(mapValuesConfig.getFilePath());
        checkOverlappingKeys();
    }

    private void processMapField(Map<String, String> map){
        if(Objects.nonNull(map)) {
            for (Map.Entry<String, String> mapEntry : map.entrySet()) {
                String key = mapEntry.getKey();
                if (key.contains(",")) {
                    parseIndividualKeys(mapEntry);
                } else if (key.contains("-")) {
                    addRangeMapping(mapEntry);
                } else {
                    addIndividualMapping(key, map.get(key));
                }
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
        try{
            String[] rangeKeys = mapEntry.getKey().split("-");
            if(rangeKeys.length!=2){
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
        catch (NumberFormatException ex){
            addIndividualMapping(mapEntry.getKey(), mapEntry.getValue());
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
            boolean matchFound = false;
            final Event recordEvent = record.getData();
            if (Objects.nonNull(mapValuesConfig.getMapWhen()) && !expressionEvaluator.evaluateConditional(mapValuesConfig.getMapWhen(), recordEvent)) {
                continue;
            }
            try {
                String matchKey = record.getData().get(mapValuesConfig.getSource(), String.class);
                if(!matchFound && individualMappings.containsKey(matchKey)){
                    //check individual keys
                    record.getData().put(mapValuesConfig.getTarget(), individualMappings.get(matchKey));
                    matchFound = true;
                }
                if(!matchFound && NumberUtils.isParsable(matchKey)){
                    //check in the range
                    Float floatKey = Float.parseFloat(matchKey);
                    for(Map.Entry<Range<Float>, String> rangeEntry : rangeMappings.entrySet()){
                        Range<Float> range = rangeEntry.getKey();
                        if(range.contains(floatKey)){
                            matchFound = true;
                            record.getData().put(mapValuesConfig.getTarget(), rangeEntry.getValue());
                            break;
                        }
                    }
                }
                if(!matchFound){
                    //check in the patterns
                    //todo
                    for(String pattern : patternMappings.keySet()){
                        if(Pattern.matches(pattern, matchKey)){
                            record.getData().put(mapValuesConfig.getTarget(), patternMappings.get(pattern));
                            break;
                        }
                    }
                }
            } catch (Exception ex){
                LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]",
                        mapValuesConfig.getSource(), recordEvent, ex);
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
}
