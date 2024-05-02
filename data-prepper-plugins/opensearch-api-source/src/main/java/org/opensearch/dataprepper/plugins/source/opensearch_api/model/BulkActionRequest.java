package org.opensearch.dataprepper.plugins.source.opensearch_api.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class BulkActionRequest {

    static final ObjectMapper objectMapper = new ObjectMapper();

    private String action;
    private Map requestModel;

    public BulkActionRequest(Map json) throws JsonProcessingException {
        this.requestModel = json;
        this.action = (String) requestModel.keySet().stream().findFirst().orElse("");
    }

    public String getId() {
        return ((Map<String, String>) this.requestModel.get(this.action)).get("_id");
    }
    public String getIndex() {
        return ((Map<String, String>) this.requestModel.get(this.action)).get("_index");
    }

    public String getAction() {
        return this.action;
    }
}
