package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;
import org.opensearch.dataprepper.typeconverter.TypeConverter;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TargetsParameterConfig {
    private final TypeConverter converter;
    private final LinkedHashMap<Range<Float>, Object> rangeMappings = new LinkedHashMap<>();
    private final Map<String, Object> individualMappings = new HashMap<>();
    private final Map<Pattern, Object> compiledPatterns = new HashMap<>();
    @JsonProperty("target")
    @NotNull
    @NotEmpty
    private String target;
    @JsonProperty("map")
    private Map<String, Object> map;
    @JsonProperty("translate_when")
    private String translateWhen;
    @JsonProperty("regex")
    private RegexParameterConfiguration regexParameterConfig;
    @JsonProperty("default")
    private String defaultValue;
    @JsonProperty("target_type")
    private TargetType targetType = TargetType.STRING;

    public TargetsParameterConfig(Map<String, Object> map, String target, RegexParameterConfiguration regexParameterConfig, String translateWhen, String defaultValue, TargetType targetType) {
        this.targetType = Optional.ofNullable(targetType).orElse(TargetType.STRING);
        this.target = target;
        this.map = map;
        this.defaultValue = defaultValue;
        this.regexParameterConfig = regexParameterConfig;
        this.converter = this.targetType.getTargetConverter();
        this.translateWhen = translateWhen;
        parseMappings();
    }

    public void parseMappings() {
        if (Objects.nonNull(getRegexParameterConfiguration())) {
            compilePatterns(getRegexParameterConfiguration().getPatterns());
        }
        processMapField(map);
        checkOverlappingKeys();
    }

    public String getTarget() {
        return target;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getTranslateWhen() {
        return translateWhen;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public RegexParameterConfiguration getRegexParameterConfiguration() {
        return regexParameterConfig;
    }

    public Map<String, Object> fetchIndividualMappings() {
        return individualMappings;
    }

    public LinkedHashMap<Range<Float>, Object> fetchRangeMappings() {
        return rangeMappings;
    }

    public Map<Pattern, Object> fetchCompiledPatterns() {
        return compiledPatterns;
    }

    public TypeConverter getConverter() {
        return converter;
    }


    @AssertTrue(message = "pattern option is mandatory while configuring regex option")
    public boolean isPatternPresent() {
        return regexParameterConfig == null || regexParameterConfig.getPatterns() != null;
    }

    @AssertTrue(message = "Either map or patterns option needs to be configured under targets.")
    public boolean hasMappings() {
        return Stream.of(map, regexParameterConfig).filter(n -> n != null).count() != 0;
    }

    @AssertTrue(message = "The mapped values do not match the target type provided")
    public boolean isMapTypeValid() {
        return map.keySet().stream().allMatch(key -> checkTargetValueType(map.get(key)));
    }

    @AssertTrue(message = "The pattern values do not match the target type provided")
    public boolean isPatternTypeValid() {
        if (Objects.isNull(regexParameterConfig) || Objects.isNull(regexParameterConfig.getPatterns())) {
            return true;
        }
        Map<String, Object> patterns = regexParameterConfig.getPatterns();
        return patterns.keySet().stream().allMatch(key -> checkTargetValueType(patterns.get(key)));
    }

    private boolean checkTargetValueType(Object val) throws NumberFormatException {
        if (Objects.isNull(targetType)) {
            return true;
        }
        try {
            final TypeConverter converter = targetType.getTargetConverter();
            converter.convert(val);
        } catch (Exception ex) {
            return false;
        }
        return true;
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
