package org.opensearch.dataprepper.pipeline.parser.transformer;

public interface PipelineTransformationPathProvider {

    String getTransformationTemplateFileLocation();

    String getTransformationRulesFileLocation();
}
