package com.amazon.situp.parser.model;

import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.parser.ConfigurationDeserializer;
import com.amazon.situp.parser.validator.PipelineComponent;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = ConfigurationDeserializer.class)
public class PipelineConfiguration {

    @PipelineComponent(type = PipelineComponent.Type.Source, message = "Invalid source configuration; Requires " +
            "exactly one valid source")
    private final Configuration source;

    @PipelineComponent(type = PipelineComponent.Type.Buffer, message = "Invalid buffer configuration; Requires" +
            " at most one valid buffer")
    private final Configuration buffer;

    @PipelineComponent(type = PipelineComponent.Type.Processor, message = "Invalid processor configuration.")
    private Configuration processor;

    @PipelineComponent(type = PipelineComponent.Type.Sink, message = "Invalid sink configuration; Requires at least " +
            "one valid sink")
    private Configuration sink;

    public PipelineConfiguration(
            Configuration source,
            Configuration buffer,
            Configuration processor,
            Configuration sink) {
        this.source = source;
        this.buffer = buffer;
        this.processor = processor;
        this.sink = sink;
    }

    public Configuration getSource() {
        return source;
    }

    public Configuration getBuffer() {
        return buffer;
    }

    public Configuration getProcessor() {
        return processor;
    }

    public Configuration getSink() {
        return sink;
    }

}
