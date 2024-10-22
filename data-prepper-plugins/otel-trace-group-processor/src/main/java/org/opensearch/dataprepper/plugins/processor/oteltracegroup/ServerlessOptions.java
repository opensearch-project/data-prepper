package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerlessOptions {
    public static final String COLLECTION_NAME = "collection_name";
    public static final String NETWORK_POLICY_NAME = "network_policy_name";
    public static final String VPCE_ID = "vpce_id";

    @JsonProperty(NETWORK_POLICY_NAME)
    private String networkPolicyName;

    @JsonProperty(COLLECTION_NAME)
    private String collectionName;

    @JsonProperty(VPCE_ID)
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
