/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;

import java.util.Objects;

@Builder
@Getter
public class NeptuneS3RecordPredicate {

    @JsonProperty("value")
    private String value;

    @JsonProperty("graph")
    private String graph;

    @JsonProperty("language")
    private String language;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NeptuneS3RecordPredicate that = (NeptuneS3RecordPredicate) o;
        return Objects.equals(value, that.value) && Objects.equals(graph, that.graph) &&
                Objects.equals(language, that.language);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, graph, language);
    }

    public static NeptuneS3RecordPredicate fromPropertGraphData(final PropertygraphData propertygraphData) {
        final String value = propertygraphData.value().asMap().get("value").asString();
        return NeptuneS3RecordPredicate
                .builder()
                .value(value)
                .build();
    }
}