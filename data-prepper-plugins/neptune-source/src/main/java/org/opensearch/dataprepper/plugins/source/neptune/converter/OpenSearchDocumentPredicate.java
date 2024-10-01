package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OpenSearchDocumentPredicate {
    
    @JsonProperty("value")
    private String value;

    @JsonProperty("graph")
    private String graph;

    @JsonProperty("language")
    private String language;

    OpenSearchDocumentPredicate(final String value, final String graph, final String language) {
        this.value = value;
        this.graph = graph;
        this.language = language;
    }
    
}
