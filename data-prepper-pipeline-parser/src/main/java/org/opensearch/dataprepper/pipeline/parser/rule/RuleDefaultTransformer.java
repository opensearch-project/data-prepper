//package org.opensearch.dataprepper.pipeline.parser.rule;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.extern.slf4j.Slf4j;
//import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
//import org.opensearch.dataprepper.pipeline.parser.transformer.TransformationFileIOException;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.Collections;
//import java.util.Optional;
//import java.util.stream.Stream;
//
//
//
////        import com.amazon.fizzyparser.wrapper.PipelineConfigurationModelWrapper;
//
//@Slf4j
//public class RuleDefaultTransformer  {
//
//    private static final String COMMENT_INDICATOR = "//";
//
//    private final ObjectMapper objectMapper = new ObjectMapper();
//
//
//    /**
//     * This default transformer will parse each line of the provided .transformation.jsonl file into a {@link com.amazon.fizzypepsi.validation.models.DefaultTransformerModel },
//     * and will put the key and value at each location found from the json_path query. At this time, this transformer assumes that all keys specified in the model at the
//     * json_path location do not exist in the model object being transformed, since any values to be set are validated to not exist beforehand.
//     * @return a TransformResult with an empty list of {@link ValidationMessage },
//     *        but in the future it may create a warning message for each json line if the value is being overridden.
//     */
//    @Override
//    public PipelinesDataFlowModel transform(final String ruleFilePath) throws TransformationFileIOException {
//
//        try (final FileInputStream fileInputStream = new FileInputStream(ruleFilePath);
//             final InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
//             final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//             final Stream<String> lines = bufferedReader.lines()) {
//
//            lines.map(line -> convertLineToModel(line))
//                    .filter(Optional::isPresent)
//                    .forEach(transformationModel -> transformWithModel(transformationModel.get(), wrapper));
//
//        } catch (final IOException e) {
//            throw new TransformationFileIOException(String.format("Failed to read transformation file %s", transformationFilePath), e);
//        }
//
//        return TransformResult.builder()
//                .withWarningMessages(Collections.emptyList())
//                .build();
//    }
//
//    private void transformWithModel(final RuleTransformerModel transformerModel,
//                                    final Integer majorVersion,
//                                    final PipelineConfigurationModelWrapper<PipelinesDataFlowModel> wrapper) {
//
//        wrapper.putValue(transformerModel.getJsonPath(), transformerModel.getKey(), transformerModel.getValue(),
//                transformerModel.getPredicates());
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
//
//    }
//
//    private boolean isEmptyLineOrComment(final String line) {
//        return line.isEmpty() || line.startsWith(COMMENT_INDICATOR);
//    }
//}
//
