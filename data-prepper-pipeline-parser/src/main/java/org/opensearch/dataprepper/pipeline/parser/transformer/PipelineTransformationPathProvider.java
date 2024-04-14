package org.opensearch.dataprepper.pipeline.parser.transformer;

public interface PipelineTransformationPathProvider {

    String getTransformationTemplateDirectoryLocation();

    String getTransformationRulesDirectoryLocation();

}
