package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterSettingsParserTest {
    private static final JsonpMapper JSONP_MAPPER = new PreSerializedJsonpMapper();
    private static final Map<String, JsonData> TEST_STRING_SETTINGS = Map.of(
            "primaryKey", JsonData.of("testValue", JSONP_MAPPER)
    );
    private static final Map<String, JsonData> TEST_NESTED_SETTINGS = Map.of(
            "primaryKey", JsonData.of(
                    Map.of("secondaryKey", Map.of("thirdKey", true)), JSONP_MAPPER)
    );
    @Mock
    private GetClusterSettingsResponse getClusterSettingsResponse;

    private final ClusterSettingsParser objectUnderTest = new ClusterSettingsParser();

    @Test
    public void testGetDefaultSetting_success() {
        when(getClusterSettingsResponse.defaults()).thenReturn(TEST_STRING_SETTINGS);
        assertEquals("testValue",
                objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "primaryKey"));
    }

    @Test
    public void testGetPersistentSetting_success() {
        when(getClusterSettingsResponse.persistent()).thenReturn(TEST_STRING_SETTINGS);
        assertEquals("testValue",
                objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "primaryKey"));
    }

    @Test
    public void testGetTransientSetting_success() {
        when(getClusterSettingsResponse.transient_()).thenReturn(TEST_STRING_SETTINGS);
        assertEquals("testValue",
                objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "primaryKey"));
    }

    @Test
    public void testGetNestedSetting_success() {
        when(getClusterSettingsResponse.defaults()).thenReturn(TEST_NESTED_SETTINGS);
        assertEquals("true",
                objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "primaryKey.secondaryKey.thirdKey"));
    }

    @Test
    public void testGetSetting_missing() {
        when(getClusterSettingsResponse.defaults()).thenReturn(TEST_STRING_SETTINGS);
        assertNull(objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "missingKey"));
    }

    @Test
    public void testGetNestedSetting_missing() {
        when(getClusterSettingsResponse.defaults()).thenReturn(TEST_NESTED_SETTINGS);
        assertNull(objectUnderTest.getStringValueClusterSetting(getClusterSettingsResponse, "primaryKey.secondaryKey.missingKey"));
    }
}