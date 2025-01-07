/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.neptune.stream.StreamUtils;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphRecord;
import software.amazon.awssdk.services.neptunedata.model.SparqlData;
import software.amazon.awssdk.services.neptunedata.model.SparqlRecord;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Unified DTO for Records fetched from Neptune Streams that supports both RDF (SPARQL) and PropertyGraphs (OC, Gremlin).
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
}
