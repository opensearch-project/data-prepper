package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;

import java.util.Arrays;

public class ClusterSettingsParser {

    public String getStringValueClusterSetting(final GetClusterSettingsResponse response, final String key) {
        final String[] keyPath = key.split("[.]");
        final String keyPrefix = keyPath[0];
        final String jsonPtrSuffix = String.join(
                String.valueOf(JsonPointer.SEPARATOR), Arrays.copyOfRange(keyPath, 1, keyPath.length));
        final String jsonPtr = jsonPtrSuffix.isEmpty() ? "" : JsonPointer.SEPARATOR + jsonPtrSuffix;
        if (response.persistent().containsKey(keyPrefix)) {
            return getStringFromJsonPointer(response.persistent().get(keyPrefix), jsonPtr);
        } else if (response.transient_().containsKey(keyPrefix)) {
            return getStringFromJsonPointer(response.transient_().get(keyPrefix), jsonPtr);
        } else if (response.defaults().containsKey(keyPrefix)) {
            return getStringFromJsonPointer(response.defaults().get(keyPrefix), jsonPtr);
        } else {
            return null;
        }
    }

    private String getStringFromJsonPointer(final JsonData jsonData, final String jsonPtr) {
        return jsonData == null ? null : jsonData.to(JsonNode.class).at(JsonPointer.compile(jsonPtr)).asText(null);
    }
}
