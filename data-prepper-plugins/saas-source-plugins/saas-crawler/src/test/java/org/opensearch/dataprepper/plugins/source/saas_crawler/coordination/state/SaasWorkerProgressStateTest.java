package org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class SaasWorkerProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDefaultValues() throws JsonProcessingException {
        String state = "{}";
        SaasWorkerProgressState workderProgressState = objectMapper.readValue(state, SaasWorkerProgressState.class);
        assert workderProgressState.getTotalItems() == 0;
        assert workderProgressState.getLoadedItems() == 0;
        assert workderProgressState.getKeyAttributes() != null;
        assert workderProgressState.getKeyAttributes().isEmpty();
        assert workderProgressState.getExportStartTime() == 0;
        assert workderProgressState.getItemIds() == null;
    }

    @Test
    void testInitializedValues() throws JsonProcessingException {
        String state = "{\"keyAttributes\":{\"project\":\"project-1\"},\"totalItems\":10,\"loadedItems\":20,\"exportStartTime\":1729391235717,\"itemIds\":[\"GTMS-25\",\"GTMS-24\"]}";
        SaasWorkerProgressState workderProgressState = objectMapper.readValue(state, SaasWorkerProgressState.class);
        assert workderProgressState.getTotalItems() == 10;
        assert workderProgressState.getLoadedItems() == 20;
        assert workderProgressState.getKeyAttributes() != null;
        assert workderProgressState.getExportStartTime() == 1729391235717L;
        assert workderProgressState.getItemIds() != null;
        assert workderProgressState.getItemIds().size() == 2;
    }
}
