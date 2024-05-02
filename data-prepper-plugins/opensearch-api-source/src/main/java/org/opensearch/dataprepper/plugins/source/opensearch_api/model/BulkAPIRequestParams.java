package org.opensearch.dataprepper.plugins.source.opensearch_api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Getter
@Builder
public class BulkAPIRequestParams {

    private final String index;
    private final String pipeline;
    private final String routing;
    private final String refresh;
}
