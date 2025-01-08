/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

public class ServerlessOptions {

    @Size(min = 1, message = "network_policy_name cannot be empty")
    @JsonProperty("network_policy_name")
    private String networkPolicyName;

    @Size(min = 1, message = "collection_name cannot be empty")
    @JsonProperty("collection_name")
    private String collectionName;

    @Size(min = 1, message = "vpce_id cannot be empty")
    @JsonProperty("vpce_id")
    private String vpceId;

    public ServerlessOptions() {

    }

    public ServerlessOptions(String networkPolicyName, String collectionName, String vpceId) {
        this.networkPolicyName = networkPolicyName;
        this.collectionName = collectionName;
        this.vpceId = vpceId;
    }

    public String getNetworkPolicyName() {
        return networkPolicyName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getVpceId() {
        return vpceId;
    }

}

