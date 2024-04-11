//package org.opensearch.dataprepper.pipeline.parser.rule;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.jayway.jsonpath.Configuration;
//import com.jayway.jsonpath.JsonPath;
//import static java.lang.String.format;
//import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
//import org.opensearch.dataprepper.pipeline.parser.ParseException;
//import org.opensearch.dataprepper.pipeline.parser.model.RuleConfig;
//import org.opensearch.dataprepper.pipeline.parser.transformer.TransformationFileIOException;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileFilter;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.List;
//import java.util.Objects;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class RuleParser {
//    private final ObjectMapper objectMapper = new ObjectMapper();
//    private static final String COMMENT_INDICATOR = "//";
//
//    private static final String rulesLocationPath = "src/resources/rules";
//    private List<String> rulesJsonPath;
//
//    public void parseRuleFile(String ruleFilePath){
//        try {
////            RuleTransformerModel ruleJson = objectMapper.readValue(ruleFile, RuleTransformerModel.class);
//
//            try (final FileInputStream fileInputStream = new FileInputStream(ruleFilePath);
//                 final InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
//                 final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//                 final Stream<String> lines = bufferedReader.lines()) {
//
//                lines.map(line -> convertLineToModel(line))
//                        .filter(Optional::isPresent)
//                        .forEach(ruleModel->rulesJsonPath.add(ruleModel.get().getJsonPath()));
//
//
//            } catch (final IOException e) {
//                throw new TransformationFileIOException(String.format("Failed to read  file %s", ruleFilePath), e);
//            }
//        } catch (TransformationFileIOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private Optional<RuleTransformerModel> convertLineToModel(final String line) {
//
//        try {
//            return Optional.of(objectMapper.readValue(line, RuleTransformerModel.class));
//        } catch (JsonProcessingException e) {
//            if (!isEmptyLineOrComment(line)) {
//                throw new RuntimeException(String.format("Json line %s cannot be converted to RuleTransformerModel", line), e);
//            }
//
//        }
//        return null;
//    }
//
//
//    private boolean isEmptyLineOrComment(final String line) {
//        return line.isEmpty() || line.startsWith(COMMENT_INDICATOR);
//    }
//
//    public List<RuleTransformerModel> getRules(){
//        //scan directory, src/main/resources/rules for all *-rule.yaml files
//        // convert them to rule
//
//        try{
//            File rulesLocation = new File(rulesLocationPath);
//
//            FileFilter yamlFilter = pathname -> (pathname.getName().endsWith(".yaml") || pathname.getName().endsWith(".yml"));
//            if(rulesLocation.isDirectory()) {
//                List<RuleTransformerModel> rules = Stream.of(rulesLocation.listFiles(yamlFilter))
//                        .map(ruleFile -> parseRuleFile(ruleFile))
//                        .filter(Objects::nonNull)
//                        .collect(Collectors.toList());
//                return rules;
//            }
//        }catch (ParseException e){
//            throw new RuntimeException(format("Template rule location not found at %s", rulesLocationPath));
//        }
//
//        return null;
//    }
//
//    //TODO??
//    public boolean evaluate(PipelinesDataFlowModel pipelinesDataFlowModel){
//        try {
//            String pipelineJson = objectMapper.writeValueAsString(pipelinesDataFlowModel);
//            Object document = Configuration.defaultConfiguration()
//                    .jsonProvider()
//                    .parse(pipelineJson);
//
//            //TODO what if
//            List<RuleTransformerModel> rules = getRules();
//
//            Boolean ruleValid = false;
//            for(RuleTransformerModel rule:rules){
//                String jsonPath = rule.getJsonPath();
//                List<?> result = JsonPath.read(document, rule.getJsonPath());
//                if (result.isEmpty() || (result.size() == 1 && result.get(0) instanceof Boolean && !(Boolean)result.get(0))) {
//                    return false;
//                }
//            }
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//        return false;
//    }
//
//}
//
