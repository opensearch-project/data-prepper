/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.opensearch.dataprepper.plugins.source.neptune.stream.StreamUtils;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;
import software.amazon.awssdk.services.neptunedata.model.SparqlData;

import java.io.IOException;

/**
 * Represents a single record stored in S3.
 */
@Builder
@Getter
public class NeptuneS3Record {

    private static final String DOCUMENT_TYPE_VERTEX = "vertex";
    private static final String DOCUMENT_TYPE_EDGE = "edge";
    private static final String DOCUMENT_TYPE_RDF = "rdf-resource";
    private static final String VERTEX_ID_PREFIX = "v://";
    private static final String EDGE_ID_PREFIX = "e://";
    private static final String PG_ENTITY_ID_KEY = "label";

    // Reference to Neptune entity corresponding to document. For Gremlin, it will be Vertex Id for Vertex document &
    // Edge ID for Edge Document. For Sparql, it will be RDF subject URI
    @JsonProperty("entity_id")
    private String entityId;

    // Store the Neptune entity type. Vertex/Edge label for gremlin. rdf:type for Sparql
    @JsonProperty("entity_type")
    private String entityType;

    // Classify Open Search document. It could be one of vertex / edge / rdf-resource
    @JsonProperty("document_type")
    private String documentType;

    // Nested Field for storing predicate corresponding to Graph vertex / Edge
    @JsonProperty("predicate")
    private ImmutablePair<String, NeptuneS3RecordPredicate> predicate;

    private static NeptuneS3Record convertPropertyGraphStreamRecord(final NeptuneStreamRecord record) {
        if (!(record.getData() instanceof PropertygraphData)) {
            throw new IllegalArgumentException("Data must be a PropertygraphData");
        }

        String entityType = null;
        ImmutablePair<String, NeptuneS3RecordPredicate> predicate = null;
        final PropertygraphData propertygraphData = ((PropertygraphData) record.getData());
        final String key = propertygraphData.key();
        if (key.equalsIgnoreCase(PG_ENTITY_ID_KEY)) {
            entityType = propertygraphData.value().asMap().get("value").asString();
        } else {
            predicate = ImmutablePair.of(key, NeptuneS3RecordPredicate.fromPropertGraphData(propertygraphData));
        }

        return NeptuneS3Record
                .builder()
                .entityId(getEntityIdForPropertyGraph(propertygraphData.type(), propertygraphData.id()))
                .documentType(getDocumentTypeForPropertyGraph(propertygraphData.type()))
                .entityType(entityType)
                .predicate(predicate)
                .build();
    }

    private static NeptuneS3Record convertSparqlStreamRecord(final NeptuneStreamRecord record) throws IOException {
        if (!(record.getData() instanceof SparqlData)) {
            throw new IllegalArgumentException("Data must be a SparqlData");
        }
        final Statement stmt = StreamUtils.parseSparqlStatement(((SparqlData) record.getData()).stmt());
        final boolean isTypeStmt = stmt.getPredicate().isIRI() && stmt.getPredicate().equals(RDF.TYPE);
        ImmutablePair<String, NeptuneS3RecordPredicate> predicate = null;
        String entityType = null;

        if (isTypeStmt) {
            entityType = stmt.getObject().stringValue();
        } else {
            final String predicateName = stmt.getPredicate().stringValue();
            final NeptuneS3RecordPredicate predicateValue = NeptuneS3RecordPredicate
                    .builder()
                    .value(stmt.getObject().stringValue())
                    .graph(stmt.getContext().stringValue())
                    .language(stmt.getObject() instanceof Literal ? ((Literal) stmt.getObject()).getLanguage().orElse(null) : null)
                    .build();
            predicate = ImmutablePair.of(predicateName, predicateValue);
        }
        return NeptuneS3Record
                .builder()
                .entityId(stmt.getSubject().stringValue())
                .documentType(DOCUMENT_TYPE_RDF)
                .entityType(entityType)
                .predicate(predicate)
                .build();

    }

    public static NeptuneS3Record fromNeptuneStreamRecord(final NeptuneStreamRecord neptuneStreamRecord) throws IOException {
        if (neptuneStreamRecord.getData() instanceof PropertygraphData) {
            return convertPropertyGraphStreamRecord(neptuneStreamRecord);
        }
        return convertSparqlStreamRecord(neptuneStreamRecord);
    }

    private static String getEntityIdForPropertyGraph(final String type, final String entityId) {
        return String.format("%s%s", type.startsWith("v") ? VERTEX_ID_PREFIX : EDGE_ID_PREFIX, entityId);
    }

    private static String getDocumentTypeForPropertyGraph(final String type) {
        if (type.equalsIgnoreCase("vl") || type.equalsIgnoreCase("vp")) {
            return DOCUMENT_TYPE_VERTEX;
        }
        return DOCUMENT_TYPE_EDGE;
    }
}
