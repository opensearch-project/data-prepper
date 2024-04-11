//package org.opensearch.dataprepper.pipeline.parser.model;
//
//import javax.inject.Inject;
//        import javax.inject.Named;
//        import java.io.File;
//        import java.util.Arrays;
//        import java.util.List;
//        import java.util.Map;
//        import java.util.regex.Matcher;
//        import java.util.regex.Pattern;
//        import java.util.stream.Collectors;
//
//        import static com.amazon.fizzypepsi.dagger.PipelineConfigurationBodyProcessorModule.TRANSFORMERS_DIRECTORY_PATH;
//import org.opensearch.dataprepper.pipeline.parser.transformer.DefaultTransformer;
//
//public class TransformersFactory {
//
//    private static final String TRANSFORMER_NAME_CAPTURE = "name";
//    private static final String TRANSFORMER_FILE_REGEX_PATTERN = "(?<name>[0-9a-z_]+)\\.transformer\\.jsonl";
//    private static final Pattern pattern = Pattern.compile(TRANSFORMER_FILE_REGEX_PATTERN);
//
//    private final DefaultTransformer defaultTransformer;
//    private final String transformersDirectoryPath;
//
//    @Inject
//    public TransformersFactory(final DefaultTransformer defaultTransformer,
//                               @Named(TRANSFORMERS_DIRECTORY_PATH) final String transformersDirectoryPath) {
//        this.defaultTransformer = defaultTransformer;
//        this.transformersDirectoryPath = transformersDirectoryPath;
//    }
//
//    public List<DataPrepperTransformerWrapper> provideDataPrepperTransformers() {
//        final File directory = new File(transformersDirectoryPath);
//        final File[] transformerDefinitionFiles = directory.listFiles();
//
//        return Arrays.stream(transformerDefinitionFiles)
//                .map(this::parseTransformerWrapperFromFile)
//                .collect(Collectors.toList());
//    }
//
//    private DataPrepperTransformerWrapper parseTransformerWrapperFromFile(final File transformerDefinitionFile) {
//        final String transformerName = getTransformerNameFromFileName(transformerDefinitionFile.getName());
//        final String filePath = transformerDefinitionFile.getPath();
//        final DataPrepperPipelineModelTransformer transformer = customTransformers.getOrDefault(transformerName, defaultTransformer);
//
//        return DataPrepperTransformerWrapper.builder()
//                .withName(transformerName)
//                .withFilePath(filePath)
//                .withTransformer(transformer)
//                .build();
//    }
//
//    private String getTransformerNameFromFileName(final String fileName) {
//        final Matcher matcher = pattern.matcher(fileName);
//
//        if (matcher.find()) {
//            return matcher.group(TRANSFORMER_NAME_CAPTURE);
//        }
//
//        throw new InvalidTransformerConfigurationException(
//                String.format("The transformer file %s did not match the expected pattern %s", fileName, TRANSFORMER_FILE_REGEX_PATTERN)
//        );
//    }
//}
