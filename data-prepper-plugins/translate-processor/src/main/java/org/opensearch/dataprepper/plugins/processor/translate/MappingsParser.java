package org.opensearch.dataprepper.plugins.processor.translate;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class MappingsParser {
    private final LinkedHashMap<Range<Float>, Object> rangeMappings = new LinkedHashMap<>();
    private final Map<String, Object> individualMappings = new HashMap<>();
    private final Map<Pattern, Object> compiledPatterns = new HashMap<>();
    public MappingsParser(TargetsParameterConfig targetConfig){
        RegexParameterConfiguration regexConfig = targetConfig.getRegexParameterConfiguration();
        if (Objects.nonNull(regexConfig)) {
            compilePatterns(regexConfig.getPatterns());
        }
        processMapField(targetConfig.getMap());
        checkOverlappingKeys();
    }

    public Map<String, Object> fetchIndividualMappings() { return individualMappings; }

    public LinkedHashMap<Range<Float>, Object> fetchRangeMappings() { return rangeMappings; }

    public Map<Pattern, Object> fetchCompiledPatterns() { return compiledPatterns; }

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

    private void parseIndividualKeys(Map.Entry<String, Object> mapEntry) {
        String[] commaSeparatedKeys = mapEntry.getKey().split(",");
        for (String individualKey : commaSeparatedKeys) {
            if (individualKey.contains("-")) {
                addRangeMapping(Map.entry(individualKey, mapEntry.getValue()));
            } else {
                addIndividualMapping(individualKey, mapEntry.getValue());
            }
        }
    }

    private void addRangeMapping(Map.Entry<String, Object> mapEntry) {
        String[] rangeKeys = mapEntry.getKey().split("-");
        if (rangeKeys.length != 2 || !StringUtils.isNumericSpace(rangeKeys[0]) || !StringUtils.isNumericSpace(rangeKeys[1])) {
            addIndividualMapping(mapEntry.getKey(), mapEntry.getValue());
        } else {
            Float lowKey = Float.parseFloat(rangeKeys[0]);
            Float highKey = Float.parseFloat(rangeKeys[1]);
            Range<Float> rangeEntry = Range.between(lowKey, highKey);
            if (isRangeOverlapping(rangeEntry)) {
                String exceptionMsg = "map option contains key " + mapEntry.getKey() + " that overlaps with other range entries";
                throw new InvalidPluginConfigurationException(exceptionMsg);
            } else {
                rangeMappings.put(Range.between(lowKey, highKey), mapEntry.getValue());
            }
        }
    }

    private void addIndividualMapping(final String key, final Object value) {
        if (individualMappings.containsKey(key)) {
            String exceptionMsg = "map option contains duplicate entries of " + key;
            throw new InvalidPluginConfigurationException(exceptionMsg);
        } else {
            individualMappings.put(key.strip(), value);
        }
    }

    private boolean isRangeOverlapping(Range<Float> rangeEntry) {
        return rangeMappings.keySet().stream().anyMatch(range -> range.isOverlappedBy(rangeEntry));
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
}
