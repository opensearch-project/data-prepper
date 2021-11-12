package org.opensearch.dataprepper.logstash.parser;

import com.amazon.dataprepper.model.configuration.PipelineModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.dataprepper.logstash.LogstashLexer;
import org.opensearch.dataprepper.logstash.LogstashParser;
import org.opensearch.dataprepper.logstash.mapping.LogstashMapper;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

/**
 * Converts Logstash configuration file and returns YAML file location
 *
 * @since 1.2
 */
public class LogstashConfigConverter {
    public String convertLogstashConfigurationToPipeline(String logstashConfigurationPath, String outputDirectory) throws IOException {
        final String logstashConfigAsString = new String(Files.readAllBytes(Paths.get(logstashConfigurationPath)));

        LogstashLexer lexer = new LogstashLexer(CharStreams.fromString(logstashConfigAsString));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        LogstashParser parser = new LogstashParser(tokens);
        ParseTree tree = parser.config();

        LogstashVisitor visitor = new LogstashVisitor();
        LogstashConfiguration logstashConfiguration = (LogstashConfiguration) visitor.visit(tree);

        LogstashMapper logstashMapper = new LogstashMapper();
        PipelineModel pipelineModel = logstashMapper.mapPipeline(logstashConfiguration);
        Map<String, Object> pipeline = Collections.singletonMap("log-pipeline", pipelineModel);

        YAMLFactory factory = new YAMLFactory();
        factory.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
        factory.enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);
        factory.disable(YAMLGenerator.Feature.SPLIT_LINES);
        factory.enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE);

        ObjectMapper mapper = new ObjectMapper(factory);

        String yamlFilePath = outputDirectory + "logstash.yaml";
        mapper.writeValue(new File(yamlFilePath), pipeline);

        return yamlFilePath;
    }
}
