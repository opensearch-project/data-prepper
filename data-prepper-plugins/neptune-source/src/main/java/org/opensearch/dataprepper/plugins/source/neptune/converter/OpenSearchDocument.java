package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@Builder
@Getter
public class OpenSearchDocument {

    public static final String VERTEX_ID_PREFIX = "v://";
    public static final String EDGE_ID_PREFIX = "e://";

    // Reference to Neptune entity corresponding to document. For Gremlin, it will be Vertex Id for Vertex document &
    // Edge ID for Edge Document. For Sparql, it will be RDF subject URI
    @JsonProperty("entity_id")
    private String entityId;

    // Store the Neptune entity type(s). Vertex/Edge label for gremlin. rdf:type for Sparql
    // Note that Gremlin Vertexes and Sparql can have multiple types.
    @JsonProperty("entity_type")
    private Set<String> entityType;

    // Classify Open Search document. It could be one of vertex / edge / rdf-resource
    @JsonProperty("document_type")
    private String documentType;

    // Nested Field for storing predicates corresponding to Graph vertex / Edge
    @JsonProperty("predicates")
    private Map<String, Set<OpenSearchDocumentPredicate>> predicates;


    // merges two opensearch documents for the same entityId
    // for ex
    // d1
    // <s1> rdf:type Person.
    // <s1> <knows> "Mohamed".

    // d2
    // <s1> rdf:type Human.
    // <s1> <knows> "Andreas".
    // <s1> <plays> "Football".

    // result
    // {entity_type: [Person, Human], predicates: {"knows": [Mohamed, Andreas], "plays": [football]}}

    public void merge(final OpenSearchDocument other) {
        // must be same entity
        if (!this.entityId.equalsIgnoreCase(other.entityId)) {
            return;
        }

        // merge labels
        this.getEntityType().addAll(other.getEntityType());

        // merge predicates
        for (final Map.Entry<String, Set<OpenSearchDocumentPredicate>> otherPredicates : other.getPredicates().entrySet()) {
            // if curr doc doesn't have the predicate, just add it
            if (!this.predicates.containsKey(otherPredicates.getKey())) {
                this.predicates.put(otherPredicates.getKey(), otherPredicates.getValue());
                continue;
            }

            // predicate exists, need to merge them
            final Set<OpenSearchDocumentPredicate> currPredicateValue = this.getPredicates().get(otherPredicates.getKey());
            currPredicateValue.addAll(otherPredicates.getValue());
        }
    }
}
