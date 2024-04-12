package org.opensearch.dataprepper.pipeline.parser.transformer;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineTemplateModel;

public interface PipelineConfigurationTransformer {
//    /**
//     * Transforms the template YAML by substituting placeholders with values from the original YAML.
//     *
//     * @param originalYaml The original YAML configuration.
//     * @param templateYaml The template YAML with placeholders.
//     * @return The transformed YAML configuration.
//     */
//    String transformYaml(String originalYaml, String templateYaml);

    PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                  PipelineTemplateModel templateModel);

}
