package org.opensearch.dataprepper.pipeline.parser;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

public interface PipelineYamlTransformer {
    /**
     * Transforms the template YAML by substituting placeholders with values from the original YAML.
     *
     * @param originalYaml The original YAML configuration.
     * @param templateYaml The template YAML with placeholders.
     * @return The transformed YAML configuration.
     */
    String transformYaml(String originalYaml, String templateYaml);

    PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                  PipelinesDataFlowModel templateDataFlowModel);

}
