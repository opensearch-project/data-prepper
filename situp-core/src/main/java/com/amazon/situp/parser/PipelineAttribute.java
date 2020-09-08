package com.amazon.situp.parser;

public enum PipelineAttribute {
    PIPELINE("pipeline"),
    NAME("name"),
    SOURCE("source"),
    BUFFER("buffer"),
    PROCESSOR("processor"),
    SINK("sink");

    private final String attributeName;

    PipelineAttribute(final String attributeName) {
        this.attributeName = attributeName;
    }

    public String attributeName() {
        return attributeName;
    }
}
