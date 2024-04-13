package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import static org.opensearch.dataprepper.pipeline.parser.PipelineTransformationConfiguration.RULES_DIRECTORY_PATH;
import static org.opensearch.dataprepper.pipeline.parser.PipelineTransformationConfiguration.TEMPLATES_DIRECTORY_PATH;

import javax.inject.Named;
import java.io.File;
import java.io.IOException;

public class TransformersFactory implements PipelineTransformationPathProvider {

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
    public String getTransformationTemplateFileLocation() {
        return templatesDirectoryPath;
    }

    @Override
    public String getTransformationRulesFileLocation() {
        return rulesDirectoryPath;
    }

    public String getPluginTemplateFileLocation(String pluginName) {
        return templatesDirectoryPath + "/" + pluginName + TEMPLATE_FILE_NAME_PATTERN;
    }

    public String getPluginRuleFileLocation(String pluginName) {
        return rulesDirectoryPath + "/" + pluginName + RULE_FILE_NAME_PATTERN;
    }
}
