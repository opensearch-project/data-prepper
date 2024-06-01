/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import static org.opensearch.dataprepper.pipeline.parser.PipelineTransformationConfiguration.RULES_DIRECTORY_PATH;
import static org.opensearch.dataprepper.pipeline.parser.PipelineTransformationConfiguration.TEMPLATES_DIRECTORY_PATH;

import javax.inject.Named;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformersFactory implements PipelineTransformationPathProvider {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private final String TEMPLATE_FILE_NAME_PATTERN = "-template.yaml";
    private final String RULE_FILE_NAME_PATTERN = "-rule.yaml";
    private final String templatesDirectoryPath;
    private final String rulesDirectoryPath;

    public TransformersFactory(@Named(RULES_DIRECTORY_PATH) final String rulesDirectoryPath,
                               @Named(TEMPLATES_DIRECTORY_PATH) final String templatesDirectoryPath) {
        this.templatesDirectoryPath = templatesDirectoryPath;
        this.rulesDirectoryPath = rulesDirectoryPath;
    }

    @Override
    public String getTransformationTemplateDirectoryLocation() {
        return templatesDirectoryPath;
    }

    @Override
    public String getTransformationRulesDirectoryLocation() {
        return rulesDirectoryPath;
    }

    public String getPluginTemplateFileLocation(String pluginName) {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new RuntimeException("Transformation plugin not found");
        }
        return templatesDirectoryPath + "/" + pluginName + TEMPLATE_FILE_NAME_PATTERN;
    }

    public InputStream getPluginTemplateFileStream(String pluginName) {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new RuntimeException("Transformation plugin not found");
        }
        ClassLoader classLoader = TransformersFactory.class.getClassLoader();
        InputStream filestream = classLoader.getResourceAsStream("templates" + "/" + pluginName + TEMPLATE_FILE_NAME_PATTERN);
        return filestream;
    }

    public List<Path> getRuleFiles() {
        // Get the URI of the rules folder
        URI uri = null;
        try {
            uri = getClass().getClassLoader().getResource("rules").toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        Path rulesFolderPath;

        if ("jar".equals(uri.getScheme())) {
            // File is inside a JAR, create a filesystem for it
            try (FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
                rulesFolderPath = fileSystem.getPath("rules");
                return scanFolder(rulesFolderPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // File is not inside a JAR
            rulesFolderPath = Paths.get(uri);
            return scanFolder(rulesFolderPath);
        }
    }

    private List<Path> scanFolder(Path folderPath) {
        List<Path> pathsList = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(folderPath)) {
            pathsList = paths
                    .filter(Files::isRegularFile) // Filter to include only regular files
                    .collect(Collectors.toList()); // Collect paths into the list
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return pathsList;
    }

    public InputStream readRuleFile(Path ruleFile) throws IOException {
        ClassLoader classLoader = TransformersFactory.class.getClassLoader();
        InputStream ruleStream = classLoader.getResourceAsStream("rules" + "/" + ruleFile.getFileName().toString());
        return ruleStream;
    }

    public PipelineTemplateModel getTemplateModel(String pluginName) {
        String templatePath = getPluginTemplateFileLocation(pluginName);

        try {
            PipelineTemplateModel pipelineTemplateModel = YAML_MAPPER.readValue(new File(templatePath), PipelineTemplateModel.class);
            return pipelineTemplateModel;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
