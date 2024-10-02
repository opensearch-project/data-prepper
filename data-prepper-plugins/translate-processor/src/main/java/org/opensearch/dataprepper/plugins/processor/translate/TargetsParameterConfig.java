package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
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
    @JsonPropertyDescription("The key that specifies the field in the output in which the translated value will be placed.")
    @NotNull
    @NotEmpty
    private String target;
    @JsonProperty("map")
    @JsonPropertyDescription("A list of key-value pairs that define the translations. Each key represents a possible " +
            "value in the source field, and the corresponding value represents what it should be translated to. " +
            "For examples, see <a href=\"#map-option\">map option</a>. At least one of <code>map</code> and <code>regex</code> should be configured.")
    private Map<String, Object> map;
    @JsonProperty("translate_when")
    @JsonPropertyDescription("Uses a <a href=\"{{site.url}}{{site.baseurl}}/data-prepper/pipelines/expression-syntax/\">Data Prepper expression</a> " +
            "to specify a condition for performing the translation. When specified, the expression will only translate when the condition is met.")
    private String translateWhen;
    @JsonProperty("regex")
    @JsonPropertyDescription("A map of keys that defines the translation map. For more options, see <a href=\"#regex-option\">regex option</a>. " +
            "At least one of <code>map</code> and <code>regex</code> should be configured.")
    private RegexParameterConfiguration regexParameterConfig;
    @JsonProperty("default")
    @JsonPropertyDescription("The default value to use when no match is found during translation.")
    private String defaultValue;
    @JsonProperty("type")
    @JsonPropertyDescription("Specifies the data type for the target value.")
    private TargetType targetType = TargetType.STRING;

    public TargetsParameterConfig(){
        converter = TargetType.STRING.getTargetConverter();
    }
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
        if(Objects.isNull(map)){
            return true;
        }
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
