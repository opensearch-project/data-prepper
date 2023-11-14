package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerlessOptions {

    @JsonProperty("network_policy_name")
    private String networkPolicyName;

    @JsonProperty("collection_name")
    private String collectionName;

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

