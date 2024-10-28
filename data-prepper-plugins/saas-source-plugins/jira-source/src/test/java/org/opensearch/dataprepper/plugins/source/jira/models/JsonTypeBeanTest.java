package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class JsonTypeBeanTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private Map<String, Object> testConfiguration;

    private JsonTypeBean jsonTypeBean;

    @BeforeEach
    public void setup() throws JsonProcessingException {
        String state = "{}";
        jsonTypeBean = objectMapper.readValue(state, JsonTypeBean.class);
    }

    @Test
    public void testConstructor() {
        assertNotNull(jsonTypeBean);

        assertNull(jsonTypeBean.getType());
        assertNull(jsonTypeBean.getItems());
        assertNull(jsonTypeBean.getSystem());
        assertNull(jsonTypeBean.getCustom());
        assertNull(jsonTypeBean.getCustomId());
        assertNull(jsonTypeBean.getConfiguration());
    }

    @Test
    public void testGetters() throws JsonProcessingException {
        String type = "typeTest";
        String items = "itemsTest";
        String system = "systemTest";
        String custom = "customTest";
        Long customId = 123L;
        Map<String, Object> jsonTypeBeanMap = new HashMap<>();
        jsonTypeBeanMap.put("type", type);
        jsonTypeBeanMap.put("items", items);
        jsonTypeBeanMap.put("system", system);
        jsonTypeBeanMap.put("custom", custom);
        jsonTypeBeanMap.put("customId", customId);
        jsonTypeBeanMap.put("configuration", testConfiguration);

        String jsonString = objectMapper.writeValueAsString(jsonTypeBeanMap);

        jsonTypeBean = objectMapper.readValue(jsonString, JsonTypeBean.class);

        assertEquals(jsonTypeBean.getType(), type);
        assertEquals(jsonTypeBean.getItems(), items);
        assertEquals(jsonTypeBean.getSystem(), system);
        assertEquals(jsonTypeBean.getCustom(), custom);
        assertEquals(jsonTypeBean.getCustomId(), customId);
        assertEquals(jsonTypeBean.getConfiguration(), testConfiguration);
    }

    @Test
    public void testEquals() throws JsonProcessingException {
        assertTrue(jsonTypeBean.equals(jsonTypeBean));

        assertFalse(jsonTypeBean.equals(null));
        assertFalse(jsonTypeBean.equals(new Object()));

        JsonTypeBean sameEntryBean;
        JsonTypeBean differentEntryBean;

        String type = "typeTest";
        String items = "itemsTest";
        String system = "systemTest";
        String custom = "customTest";
        Long customId = 123L;

        Map<String, Object> jsonTypeBeanMap = new HashMap<>();
        jsonTypeBeanMap.put("type", type);
        jsonTypeBeanMap.put("items", items);
        jsonTypeBeanMap.put("system", system);
        jsonTypeBeanMap.put("custom", custom);
        jsonTypeBeanMap.put("customId", customId);
        jsonTypeBeanMap.put("configuration", testConfiguration);

        String jsonString = objectMapper.writeValueAsString(jsonTypeBeanMap);

        jsonTypeBean = objectMapper.readValue(jsonString, JsonTypeBean.class);
        sameEntryBean = objectMapper.readValue(jsonString, JsonTypeBean.class);

        for (String key : jsonTypeBeanMap.keySet()) {
            String oldString = "";
            if (key.equals("customId")) {
                jsonTypeBeanMap.put(key, 456L);
            } else if (key.equals("configuration")) {
                Map<String, Object> differentTestConfiguration = new HashMap<>();
                differentTestConfiguration.put("differentKey", new JsonTypeBean());
                jsonTypeBeanMap.put("configuration", differentTestConfiguration);
            } else {
                oldString = jsonTypeBeanMap.get(key).toString();
                jsonTypeBeanMap.put(key, "different");
            }
            differentEntryBean = objectMapper.readValue(objectMapper.writeValueAsString(jsonTypeBeanMap), JsonTypeBean.class);
            assertEquals(jsonTypeBean, sameEntryBean);
            assertNotEquals(jsonTypeBean, differentEntryBean);
            if (key.equals("customId")) {
                jsonTypeBeanMap.put(key, 123);
            } else if (key.equals("configuration")) {
                jsonTypeBeanMap.put("configuration", testConfiguration);
            } else {
                jsonTypeBeanMap.put(key, oldString);
            }
        }
    }

    @Test
    public void testHashCode() throws JsonProcessingException {
        assertTrue(jsonTypeBean.hashCode() > 0);
        String state = "{\"type\": \"same\"}";
        String state2 = "{\"type\": \"different\"}";

        JsonTypeBean sameEntryBean;
        JsonTypeBean differentEntryBean;

        jsonTypeBean = objectMapper.readValue(state, JsonTypeBean.class);
        sameEntryBean = objectMapper.readValue(state, JsonTypeBean.class);
        differentEntryBean = objectMapper.readValue(state2, JsonTypeBean.class);

        assertEquals(jsonTypeBean.hashCode(), sameEntryBean.hashCode());
        assertNotEquals(jsonTypeBean.hashCode(), differentEntryBean.hashCode());
    }

    @Test
    public void testToString() throws JsonProcessingException {
        String state = "{\"type\": \"same\"}";
        jsonTypeBean = objectMapper.readValue(state, JsonTypeBean.class);
        String jsonString = jsonTypeBean.toString();
        assertTrue(jsonString.contains(JsonTypeBean.class.getSimpleName()));
        assertTrue(jsonString.contains("type: same"));
        assertTrue(jsonString.contains("items: null"));
        assertTrue(jsonString.contains("system"));
        assertTrue(jsonString.contains("custom"));
        assertTrue(jsonString.contains("customId"));
        assertTrue(jsonString.contains("configuration"));

    }
}

