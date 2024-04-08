package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import static java.lang.String.format;
import org.opensearch.dataprepper.pipeline.parser.ParseException;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleConfig;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RuleParser {
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private static final String rulesLocationPath = "src/resources/rules";

    public RuleConfig parseRuleFile(File ruleFile){
        try {
            return yamlMapper.readValue(ruleFile, RuleConfig.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<RuleConfig> getRules(){
        //scan directory, src/main/resources/rules for all *-rule.yaml files
        // convert them to rule

        try{
            File rulesLocation = new File(rulesLocationPath);

            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
            if(rulesLocation.isDirectory()) {
                List<RuleConfig> rules = Stream.of(rulesLocation.listFiles(yamlFilter))
                        .map(ruleFile -> parseRuleFile(ruleFile))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                return rules;
            }
        }catch (ParseException e){
            throw new RuntimeException(format("Template rule location not found at %s", rulesLocationPath));
        }

        return null;
    }

}

