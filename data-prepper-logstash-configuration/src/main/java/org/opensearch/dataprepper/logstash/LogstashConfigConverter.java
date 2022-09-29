/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash;

import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.dataprepper.logstash.mapping.LogstashMapper;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.parser.ModelConvertingLogstashVisitor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Converts Logstash configuration file and returns YAML file location
 *
 * @since 1.2
 */
public class LogstashConfigConverter {
    public String convertLogstashConfigurationToPipeline(String logstashConfigurationPath, String outputDirectory) throws IOException {
        final Path configurationFilePath = Paths.get(logstashConfigurationPath);
        final String logstashConfigAsString = new String(Files.readAllBytes(configurationFilePath));

        LogstashLexer lexer = new LogstashLexer(CharStreams.fromString(logstashConfigAsString));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        LogstashParser parser = new LogstashParser(tokens);
        final ParseTree tree = parser.config();

        ModelConvertingLogstashVisitor visitor = new ModelConvertingLogstashVisitor();
        LogstashConfiguration logstashConfiguration = (LogstashConfiguration) visitor.visit(tree);

        LogstashMapper logstashMapper = new LogstashMapper();
        PipelineModel pipelineModel = logstashMapper.mapPipeline(logstashConfiguration);
        PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                Collections.singletonMap("logstash-converted-pipeline", pipelineModel)
        );

        ObjectMapper mapper = new ObjectMapper(YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR)
                .enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)
                .disable(YAMLGenerator.Feature.SPLIT_LINES)
                .enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE)
                .build());

        final String confFileName = configurationFilePath.getFileName().toString();
        final String yamlFileName = confFileName.substring(0, confFileName.lastIndexOf(".conf"));

        final Path yamlFilePath = Paths.get(outputDirectory , yamlFileName + ".yaml");

        mapper.writeValue(new File(String.valueOf(yamlFilePath)), pipelinesDataFlowModel);

        return String.valueOf(yamlFilePath);
    }
}
