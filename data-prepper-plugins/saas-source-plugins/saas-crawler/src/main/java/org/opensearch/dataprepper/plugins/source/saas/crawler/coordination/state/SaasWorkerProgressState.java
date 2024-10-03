/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class SaasWorkerProgressState {

    @JsonProperty("totalItems")
    private int totalItems;

    @JsonProperty("loadedItems")
    private int loadedItems;

    @JsonProperty("exportStartTime")
    private long exportStartTime;

    @JsonProperty("itemIds")
    private List<String> itemIds;

}
