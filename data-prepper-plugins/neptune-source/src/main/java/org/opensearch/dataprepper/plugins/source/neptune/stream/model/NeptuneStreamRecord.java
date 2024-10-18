/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import lombok.Builder;
import lombok.Getter;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocument;
import org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocumentPredicate;
import org.opensearch.dataprepper.plugins.source.neptune.stream.StreamUtils;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphRecord;
import software.amazon.awssdk.services.neptunedata.model.SparqlData;
import software.amazon.awssdk.services.neptunedata.model.SparqlRecord;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocument.EDGE_ID_PREFIX;
import static org.opensearch.dataprepper.plugins.source.neptune.converter.OpenSearchDocument.VERTEX_ID_PREFIX;

/**
 * @param <T> Type of the data returned from the SDK currently either {@link SparqlData} or {@link PropertygraphData}.
 */
@Builder
@Getter
public class NeptuneStreamRecord<T extends ToCopyableBuilder> {
    private final Long commitTimestampInMillis;
    private final Map<String, String> eventId;
    private final T data;
    private final String op;
    private final String id;
    private final Long commitNum;
    private final Long opNum;
    private final Boolean isLastOp;

    public static <T> NeptuneStreamRecord fromStreamRecord(final T record) {
        if (record instanceof SparqlRecord) {
            return fromSparqlRecord((SparqlRecord) record);
        } else if (record instanceof PropertygraphRecord) {
            return fromPropertyGraphRecord((PropertygraphRecord) record);
        }
        throw new IllegalArgumentException("Unsupported record type: " + record.getClass());
    }

    public static NeptuneStreamRecord<SparqlData> fromSparqlRecord(final SparqlRecord sparqlRecord) {
        final String id;
        try {
            id = StreamUtils.getSparqlSubject(sparqlRecord.data().stmt());
        } catch (IOException e) {
            throw new RuntimeException("Failed to extract sparql subject.");
        }
        return NeptuneStreamRecord.<SparqlData>builder()
                .op(sparqlRecord.op())
                .commitTimestampInMillis(sparqlRecord.commitTimestampInMillis())
                .isLastOp(sparqlRecord.isLastOp())
                .eventId(sparqlRecord.eventId())
                .data(sparqlRecord.data())
                .id(id)
                .commitNum(Long.parseLong(sparqlRecord.eventId().get("commitNum")))
                .opNum(Long.parseLong(sparqlRecord.eventId().get("opNum")))
                .build();
    }

    public static NeptuneStreamRecord<PropertygraphData> fromPropertyGraphRecord(final PropertygraphRecord propertygraphRecord) {
        return NeptuneStreamRecord
                .<PropertygraphData>builder()
                .op(propertygraphRecord.op())
                .commitTimestampInMillis(propertygraphRecord.commitTimestampInMillis())
                .isLastOp(propertygraphRecord.isLastOp())
                .data(propertygraphRecord.data())
                .id(propertygraphRecord.data().id())
                .eventId(propertygraphRecord.eventId())
                .commitNum(Long.parseLong(propertygraphRecord.eventId().get("commitNum")))
                .opNum(Long.parseLong(propertygraphRecord.eventId().get("opNum")))
                .build();
    }


    private OpenSearchDocument toSparqlOpensearchDocument() throws IOException {
        if (!(this.getData() instanceof SparqlData)) {
            // fail
            throw new IllegalArgumentException("Data is not a SparqlRecord");
        }
        final Statement stmt = StreamUtils.parseSparqlStatement(((SparqlData) this.getData()).stmt());
        final boolean isTypeStmt = stmt.getPredicate().isIRI() && stmt.getPredicate().equals(RDF.TYPE);
        final Set<String> entityType = new HashSet<>();
        final Map<String, Set<OpenSearchDocumentPredicate>> predicates = new HashMap<>();

        if (isTypeStmt) {
            entityType.add(stmt.getObject().stringValue());
        } else {
            // not type stmt need to convert to predicate
            predicates.putIfAbsent(stmt.getPredicate().stringValue(), new HashSet<>());
            predicates.get(stmt.getPredicate().stringValue()).add(OpenSearchDocumentPredicate
                    .builder()
                    .value(stmt.getObject().stringValue())
                    .graph(stmt.getContext().stringValue())
                    .language(stmt.getObject() instanceof Literal ? ((Literal) stmt.getObject()).getLanguage().orElse(null) : null)
                    .build());
        }
        return OpenSearchDocument
                .builder()
                .entityId(stmt.getSubject().stringValue())
                .documentType("rdf-resource")
                .entityType(entityType)
                .predicates(predicates)
                .build();
    }

    private static String getEntityIdForPropertyGraph(final String type, final String entityId) {
        return (type.startsWith("v") ? VERTEX_ID_PREFIX : EDGE_ID_PREFIX) + entityId;
    }

    private static String getDocumentTypeForPropertyGraph(final String type) {
        if (type.equalsIgnoreCase("vl") || type.equalsIgnoreCase("vp")) {
            return "vertex";
        }
        return "edge";
    }

    private OpenSearchDocument toOpensearchPropertyGraphDocument() {
        if (!(this.getData() instanceof PropertygraphData)) {
            // fail
            throw new IllegalArgumentException("Data is not a PropertygraphRecord");
        }

        final Set<String> entityType = new HashSet<>();
        final Map<String, Set<OpenSearchDocumentPredicate>> predicates = new HashMap<>();
        final PropertygraphData propertygraphData = ((PropertygraphData) this.getData());
        final String key = (propertygraphData.key());
        if (key.equalsIgnoreCase("label")) {
            entityType.add(propertygraphData.value().asMap().get("value").asString());
        } else {
            // TODO:: handle datatype
            predicates.put(key, new HashSet<>());
            predicates.get(key).add(OpenSearchDocumentPredicate.fromPropertGraphData(propertygraphData));
        }

        return OpenSearchDocument
                .builder()
                .entityId(getEntityIdForPropertyGraph(propertygraphData.type(), propertygraphData.id()))
                .documentType(getDocumentTypeForPropertyGraph(propertygraphData.type()))
                .entityType(entityType)
                .predicates(predicates)
                .build();
    }

    public OpenSearchDocument toNeptuneOpensearchDocument() throws IOException {
        if (this.getData() instanceof SparqlData) {
            return toSparqlOpensearchDocument();
        }
        return toOpensearchPropertyGraphDocument();
    }
}
