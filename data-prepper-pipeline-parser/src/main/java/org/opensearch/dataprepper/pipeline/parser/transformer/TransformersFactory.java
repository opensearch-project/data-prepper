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

public class TransformersFactory implements PipelineTransformationPathProvider {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private final String TEMPLATE_FILE_NAME_PATTERN = "-template.yaml";
    private final String RULE_FILE_NAME_PATTERN = "-rule.yaml";
    private final String templatesDirectoryPath;
    private final String rulesDirectoryPath;
    String PLUGIN_NAME = null;

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
        if(pluginName == null || pluginName.isEmpty()){
            throw  new RuntimeException("Transformation plugin not found");
        }
        return templatesDirectoryPath + "/" + pluginName + TEMPLATE_FILE_NAME_PATTERN;
    }

    public String getPluginRuleFileLocation(String pluginName) {
        if(pluginName.isEmpty()){
            throw  new RuntimeException("Transformation plugin not found");
        }
        return rulesDirectoryPath + "/" + pluginName + RULE_FILE_NAME_PATTERN;
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
