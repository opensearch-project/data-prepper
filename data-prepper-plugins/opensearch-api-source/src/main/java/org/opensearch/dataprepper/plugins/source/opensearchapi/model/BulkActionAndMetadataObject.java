package org.opensearch.dataprepper.plugins.source.opensearchapi.model;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class BulkActionAndMetadataObject {
    @Getter
    private String action;

    private Map<String, Object> requestModel;
    private static final String emptyStringLiteral = "";

    public BulkActionAndMetadataObject(Map<String, Object> requestModel) {
        this.requestModel = requestModel;
        this.action = isRequestModelValid() ?
                requestModel.keySet().stream().findFirst().orElse(emptyStringLiteral) : emptyStringLiteral;
    }

    public String getDocId() {
        return getKeyInNestedMap("_id");
    }
    public String getIndex() {
        return getKeyInNestedMap("_index");
    }

    private String getKeyInNestedMap(final String key) {
        if (!isRequestModelValid()) return emptyStringLiteral;

        Object apiAttributesMap = requestModel.getOrDefault(this.action, new HashMap<String, Object>());
        if (!(apiAttributesMap instanceof Map)) return emptyStringLiteral;

        return ((Map<String, String>) apiAttributesMap).getOrDefault(key, emptyStringLiteral);
    }

    private boolean isRequestModelValid() {
        return requestModel != null && !requestModel.isEmpty();
    }

}
