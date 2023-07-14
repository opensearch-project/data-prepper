package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.Range;
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
        this.targetType = Optional
                .ofNullable(targetType)
                .orElse(TargetType.STRING);
        this.target = target;
        this.map = map;
        this.defaultValue = defaultValue;
        this.regexParameterConfig = regexParameterConfig;
        this.converter = this.targetType.getTargetConverter();
        this.translateWhen = translateWhen;
        parseMappings();
    }

    public String getTarget() {
        return target;
    }

    public Map<String, Object> getMap() {
        return map;
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

    public void parseMappings() {
        MappingsParser parser = new MappingsParser(this);
        individualMappings.putAll(parser.fetchIndividualMappings());
        rangeMappings.putAll(parser.fetchRangeMappings());
        compiledPatterns.putAll(parser.fetchCompiledPatterns());
    }

}
