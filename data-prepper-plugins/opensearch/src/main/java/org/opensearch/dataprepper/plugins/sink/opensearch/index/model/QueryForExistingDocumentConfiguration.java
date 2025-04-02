package org.opensearch.dataprepper.plugins.sink.opensearch.index.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.time.Duration;

public class QueryForExistingDocumentConfiguration {

    @Getter
    @JsonProperty("query_when")
    @NotNull
    private String queryWhen;

    @Getter
    @JsonProperty("query_duration")
    private Duration queryDuration = Duration.ofMinutes(3);

    @Getter
    @JsonProperty("query_term")
    @NotNull
    private String queryTerm;

    @Getter
    @JsonProperty("query_on_bulk_errors")
    private boolean queryOnBulkErrors;

    @Getter
    @JsonProperty("async_limit")
    private Integer asyncDocumentLimit = 5000;
}
