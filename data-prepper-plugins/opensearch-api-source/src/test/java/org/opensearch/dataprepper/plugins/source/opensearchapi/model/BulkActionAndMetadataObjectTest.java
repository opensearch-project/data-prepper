package org.opensearch.dataprepper.plugins.source.opensearchapi.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BulkActionAndMetadataObjectTest {

    private final String bulkActionBodyTwoLevelMap = "{\"index\": {\"_index\": \"test-index-1\"}}";
    private final String bulkActionBodyOneLevelMap = "{\"text\": \"message-1\"}";
    private ObjectMapper objectMapper;
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};
    private static final String emptyStringLiteral = "";
    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testDeserializedJsonToTwoLevelMap() throws Exception {
        Map<String, Object> bulkActionBodyMap = objectMapper.readValue(bulkActionBodyTwoLevelMap, MAP_TYPE_REFERENCE);
        BulkActionAndMetadataObject BulkActionAndMetadataObject = new BulkActionAndMetadataObject(bulkActionBodyMap);
        assertEquals(BulkActionAndMetadataObject.getAction(), "index");
        assertEquals(BulkActionAndMetadataObject.getIndex(), "test-index-1");
        assertEquals(BulkActionAndMetadataObject.getDocId(), emptyStringLiteral);
    }

    @Test
    public void testDeserializedJsonToOneLevelMap() throws Exception {
        Map<String, Object> bulkActionBodyMap = objectMapper.readValue(bulkActionBodyOneLevelMap, MAP_TYPE_REFERENCE);
        BulkActionAndMetadataObject BulkActionAndMetadataObject = new BulkActionAndMetadataObject(bulkActionBodyMap);
        assertEquals(BulkActionAndMetadataObject.getAction(), "text");
        assertEquals(BulkActionAndMetadataObject.getIndex(), emptyStringLiteral);
        assertEquals(BulkActionAndMetadataObject.getDocId(), emptyStringLiteral);
    }

    @Test
    public void testDeserializedJsonToEmptyMap() {
        BulkActionAndMetadataObject BulkActionAndMetadataObject = new BulkActionAndMetadataObject(new HashMap<>());
        assertEquals(BulkActionAndMetadataObject.getAction(), emptyStringLiteral);
        assertEquals(BulkActionAndMetadataObject.getIndex(), emptyStringLiteral);
        assertEquals(BulkActionAndMetadataObject.getDocId(), emptyStringLiteral);
    }

    @Test
    public void testDeserializedJsonToNullMap() {
        BulkActionAndMetadataObject BulkActionAndMetadataObject = new BulkActionAndMetadataObject(null);
        assertEquals(BulkActionAndMetadataObject.getAction(), emptyStringLiteral);
        assertEquals(BulkActionAndMetadataObject.getIndex(), emptyStringLiteral);
        assertEquals(BulkActionAndMetadataObject.getDocId(), emptyStringLiteral);
    }
}
