package org.opensearch.dataprepper.http.codec;

import com.linecorp.armeria.common.HttpData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiLineJsonCodecTest {
    private final HttpData serializedRequest = HttpData.ofUtf8("" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }");

    private final HttpData serializedRequestMultipleRows = HttpData.ofUtf8("" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }\n" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }\n" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }\n" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }\n" +
            "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
            "{ \"text\": \"text1\", \"year\": \"2013\" }\n");

    private final HttpData serializedRequestBad = HttpData.ofUtf8("{}\n\n{}");
    private final HttpData serializedRequestBadWithBlanks = HttpData.ofUtf8("{}\n   \n    ");
    private final HttpData serializedRequestBadWithWhiteSpaces = HttpData.ofUtf8("\t\n\r\f {}");
    private final HttpData serializedRequestBadEmpty = HttpData.ofUtf8("");
    private final HttpData serializedRequestBadEmptyNewLines = HttpData.ofUtf8("\n\n\n\n\n\n\n     \n");
    private final HttpData serializedRequestBadInvalidJson = HttpData.ofUtf8("{\"text\":");

    private final MultiLineJsonCodec multiLineJsonCodec = new MultiLineJsonCodec();

    @Test
    public void testParseSuccess() throws IOException {
        // When
        List<Map<String, Object>> res = multiLineJsonCodec.parse(serializedRequest);

        // Then
        assertEquals(2, res.size());
        assertEquals(res.get(0).containsKey("index"), true);
        Map<String, Object> innerMap = (Map<String, Object>) res.get(0).get("index");
        assertEquals(innerMap.get("_index"), "test-index");
        assertEquals(innerMap.get("_id"), "id1");
        assertEquals(res.get(1).containsKey("text"), true);
        assertEquals(res.get(1).get("text"), "text1");
        assertEquals(res.get(1).get("year"), "2013");
    }

    @Test
    public void testParseSuccess2() throws IOException {
        // When
        List<Map<String, Object>> res = multiLineJsonCodec.parse(serializedRequestMultipleRows);


        // Then
        assertEquals(10, res.size());

        for (int idx = 0; idx < res.size() - 1; idx++) {
            assertEquals(res.get(idx).containsKey("index"), true);
            Map<String, Object> innerMap = (Map<String, Object>) res.get(idx).get("index");
            assertEquals(innerMap.get("_index"), "test-index");
            assertEquals(innerMap.get("_id"), "id1");
            assertEquals(res.get(idx+1).containsKey("text"), true);
            assertEquals(res.get(idx+1).get("text"), "text1");
            assertEquals(res.get(idx+1).get("year"), "2013");
            idx++;
        }
    }

    @Test
    public void testParseFailure() {
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBad));
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBadEmpty));
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBadEmptyNewLines));
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBadInvalidJson));
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBadWithBlanks));
        assertThrows(IOException.class, () -> multiLineJsonCodec.parse(serializedRequestBadWithWhiteSpaces));
    }

}
