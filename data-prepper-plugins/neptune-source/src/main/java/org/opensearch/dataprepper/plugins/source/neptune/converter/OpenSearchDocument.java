/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenSearchDocument {

    private static final String VERTEX_ID_PREFIX = "v://";
    private static final String EDGE_ID_PREFIX = "e://";

    // Reference to Neptune entity corresponding to document. For Gremlin, it will be Vertex Id for Vertex document &
    // Edge Id for Edge Document. For Sparql, it will be RDF subject URI
    @JsonProperty("entity_id")
    private String entityId;

    // Store the Neptune entity type(s). Vertex/Edge label for gremlin. rdf:type for Sparql
    // Note that Gremlin Vertexes and Sparql can have multiple types.
    @JsonProperty("entity_type")
    private List<String> entityType;

    // Classify Open Search document. It could be one of vertex / edge / rdf-resource
    @JsonProperty("document_type")
    private String documentType;

    // Nested Field for storing predicates corresponding to Graph vertex / Edge
    @JsonProperty("predicates")
    private Map<String, List<OpenSearchDocumentPredicate>> predicates;

    private OpenSearchDocument(final String entityId, final List<String> entityType, final String documentType, final Map<String, List<OpenSearchDocumentPredicate>> predicates) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.documentType = documentType;
        this.predicates = predicates;
    }

    public static OpenSearchDocument fromStreamRecord(StreamRecord streamRecord) {
        // FIXME!!! this is an 1:1 mapping, we should group stream data first then convert to OS document

        final String type = streamRecord.getData().getType(); // TODO: map this to vertex/edge/rdf-resource
        final String id = streamRecord.getData().getId();
        final String key = streamRecord.getData().getKey();
        final String value = streamRecord.getData().getValue().get("value");
        List<String> labels = new ArrayList<>();
        Map<String, List<OpenSearchDocumentPredicate>> predicates = new HashMap<>();
        // FIXME: this only works for PG data, make it work for RDF resource
        if (key == "label") {
            labels.add(value);
        } else {
            predicates.put(key, ImmutableList.of(new OpenSearchDocumentPredicate(value, "", "de" /* FIXME: SPARQL langString */)));
        }
        return new OpenSearchDocument(
                (type.startsWith("v") ? VERTEX_ID_PREFIX : EDGE_ID_PREFIX) + id,
                labels, type, predicates
        );
    }
}

