

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;


@DataPrepperPlugin(name = "map-values", pluginType = Processor.class, pluginConfigurationType = MapValuesConfig.class)
public class MapValues extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(MapValues.class);
    private final ExpressionEvaluator expressionEvaluator;
    private final MapValuesConfig config;
    private final LinkedHashMap<Range<Integer>, String> RANGES;
    private final Map<String, String> MAP;

    private final HashMap<String, String> PATTERNS;
    @DataPrepperPluginConstructor
    public MapValues(PluginMetrics pluginMetrics, final MapValuesConfig mapValuesConfig, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = mapValuesConfig;
        this.expressionEvaluator = expressionEvaluator;
        // LOG.info("Entered mapvalues constructor");
        MAP = new HashMap<>();
        RANGES = new LinkedHashMap<>();
        PATTERNS = config.getPatterns();
        processMapField(mapValuesConfig.getMap());
        parseFile(mapValuesConfig.getFilePath());

    }

    private void processMapField(Map<String, String> map){
        for(Map.Entry<String, String> mapEntry : map.entrySet()){
            String key = mapEntry.getKey();
            if(key.contains(",")){
                parseKeys(mapEntry);
            }
            else if(key.contains("-")){
                parseRanges(mapEntry);
            }
            else{
                MAP.put(key, map.get(key));
            }
        }
    }

    private void parseKeys(Map.Entry<String, String>  mapEntry){
        String commaRegex = "([^,]+)(?:,|$)";
        Pattern pattern = Pattern.compile(commaRegex);
        Matcher matcher = pattern.matcher(mapEntry.getKey());
        while (matcher.find()) {
            String individualKey = matcher.group(1);
            MAP.put(individualKey, mapEntry.getValue());
        }
    }
    private void parseRanges(Map.Entry<String, String>  mapEntry){
        try{
            String rangeRegex = "^(\\d+)-(\\d+)$";
            Pattern pattern = Pattern.compile(rangeRegex);
            Matcher matcher = pattern.matcher(mapEntry.getKey());
            if (matcher.find()) {
                Integer low = Integer.parseInt(matcher.group(1));
                Integer high = Integer.parseInt(matcher.group(2));
                Range<Integer> rangeEntry = Range.between(low,high);
                if(isOverlapping(rangeEntry)){
                    LOG.error("map option contains a range [{}] which overlaps with other range entries.",
                            mapEntry.getKey());
                }
                else{
                    RANGES.put(Range.between(low,high), mapEntry.getValue());
                }
            }
        }
        catch (NumberFormatException ex){
            LOG.error("map option contains a range which is not allowed : [{}].",
                    mapEntry.getKey(), ex);
        }
    }

    private boolean isOverlapping(Range<Integer> rangeEntry){
        for(Range<Integer> range : RANGES.keySet()){
            if(range.isOverlappedBy(rangeEntry)){
                return true;
            }
        }
        return false;
    }

    private void parseFile(String filePath){
        //todo
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        //todo
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (Objects.nonNull(config.getMapWhen()) && !expressionEvaluator.evaluateConditional(config.getMapWhen(), recordEvent)) {
                continue;
            }

            try {
                String matchKey = record.getData().get(config.getSource(), String.class);
                if(MAP.containsKey(matchKey)){
                    //check individual keys
                    record.getData().put(config.getTarget(), MAP.get(matchKey));
                }
                else if(NumberUtils.isDigits(matchKey)){
                    //check in the range
                    Integer intKey = Integer.parseInt(matchKey);
                    for(Map.Entry<Range<Integer>, String> rangeEntry : RANGES.entrySet()){
                        Range<Integer> range = rangeEntry.getKey();
                        if(range.contains(intKey)){
                            record.getData().put(config.getTarget(), rangeEntry.getValue());
                        }
                    }
                }
                else{
                    System.out.println("Checking patterns");
                    //check in the patterns
                    for(Map.Entry<String, String> pattern : PATTERNS.entrySet()){
                        String patternKey = pattern.getKey();
                        System.out.println(patternKey);
                        //(config.getExact()!= null && Boolean.FALSE.equals(config.getExact()) && patternKey.contains(matchKey)) ||
                        if(Pattern.matches(pattern.getKey(), matchKey)){
                            record.getData().put(config.getTarget(), pattern.getValue());
                        }
                    }
                }
            } catch (Exception ex){
                LOG.error(EVENT, "Error mapping the source [{}] of entry [{}]",
                        config.getSource(), recordEvent, ex);
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
