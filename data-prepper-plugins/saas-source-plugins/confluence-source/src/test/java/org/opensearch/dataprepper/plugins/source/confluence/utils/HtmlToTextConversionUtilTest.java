package org.opensearch.dataprepper.plugins.source.confluence.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HtmlToTextConversionUtilTest {

    private ObjectMapper objectMapper;
    private ObjectNode jsonObject;

    private static Stream<Arguments> provideDifferentTypeValues() {
        return Stream.of(
                Arguments.of(42, "number"),
                Arguments.of(true, "boolean"),
                Arguments.of(3.14, "number"),
                Arguments.of("string value", "string")
        );
    }

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        jsonObject = objectMapper.createObjectNode();
    }

    @Test
    void convertHtmlToText_WithValidHtmlAndPath_ShouldConvertSuccessfully() {
        // Arrange
        jsonObject.put("content", "<p>Hello World</p>");
        String path = "content";

        // Act
        JsonNode result = HtmlToTextConversionUtil.convertHtmlToText(jsonObject, path);

        // Assert
        assertTrue(result.has("content"));
        assertEquals("Hello World", result.get("content").textValue());
    }

    @Test
    void convertHtmlToText_WithNestedPath_ShouldConvertSuccessfully() {
        // Arrange
        ObjectNode nestedNode = objectMapper.createObjectNode();
        nestedNode.put("body", "<p>Hello World</p>");
        jsonObject.set("content", nestedNode);
        String path = "content/body";

        // Act
        JsonNode result = HtmlToTextConversionUtil.convertHtmlToText(jsonObject, path);

        // Assert
        assertTrue(result.has("content"));
        assertTrue(result.get("content").has("body"));
        assertEquals("Hello World", result.get("content").get("body").textValue());
    }

    @Test
    void setValueAtGivenPath_WithSimplePath_ShouldSetValue() {
        // Arrange
        String path = "title";
        String value = "Test Title";

        // Act
        HtmlToTextConversionUtil.setValueAtGivenPath(jsonObject, path, value);

        // Assert
        assertEquals(value, jsonObject.get("title").asText());
    }

    @Test
    void setValueAtGivenPath_WithNestedPath_ShouldSetValue() {
        // Arrange
        jsonObject.putObject("content").putObject("body");
        String path = "content/body/text";
        String value = "Test Content";

        // Act
        HtmlToTextConversionUtil.setValueAtGivenPath(jsonObject, path, value);

        // Assert
        assertEquals(value, jsonObject.get("content").get("body").get("text").asText());
    }

    @Test
    void convertHtmlToText_WithNullHtml_ShouldReturnEmptyString() {
        // Act
        String result = HtmlToTextConversionUtil.convertHtmlToText(null);

        // Assert
        assertEquals("", result);
    }

    @Test
    void convertHtmlToText_WithEmptyHtml_ShouldReturnEmptyString() {
        // Act
        String result = HtmlToTextConversionUtil.convertHtmlToText("");

        // Assert
        assertEquals("", result);
    }

    @Test
    void convertHtmlToText_WithComplexHtml_ShouldConvertToPlainText() {
        // Arrange
        String html = "<div><h1>Title</h1><p>This is a <b>test</b> paragraph</p></div>";

        // Act
        String result = HtmlToTextConversionUtil.convertHtmlToText(html);

        // Assert
        assertEquals("Title This is a test paragraph", result.trim());
    }

    @Test
    void convertHtmlToText_WithInvalidPath_ShouldReturnOriginalJson() {
        // Arrange
        jsonObject.put("content", "<p>Hello World</p>");
        String invalidPath = "invalid.path";

        // Act
        JsonNode result = HtmlToTextConversionUtil.convertHtmlToText(jsonObject, invalidPath);

        // Assert
        assertEquals(jsonObject, result);
    }

    @Test
    void getValueAtGivenPath_WithSimplePath_ShouldReturnValue() {
        // Arrange
        String expectedValue = "test value";
        jsonObject.put("key", expectedValue);

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(jsonObject, "key");

        // Assert
        assertTrue(result.isPresent());
        assertEquals(expectedValue, result.get().asText());
    }

    @Test
    void getValueAtGivenPath_WithNestedPath_ShouldReturnValue() {
        // Arrange
        String expectedValue = "nested value";
        ObjectNode nestedNode = jsonObject.putObject("parent");
        nestedNode.put("child", expectedValue);

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(jsonObject, "parent/child");

        // Assert
        assertTrue(result.isPresent());
        assertEquals(expectedValue, result.get().asText());
    }

    @Test
    void getValueAtGivenPath_WithDeeplyNestedPath_ShouldReturnValue() {
        // Arrange
        String expectedValue = "deeply nested value";
        ObjectNode level1 = jsonObject.putObject("level1");
        ObjectNode level2 = level1.putObject("level2");
        level2.put("level3", expectedValue);

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(
                jsonObject, "level1/level2/level3");

        // Assert
        assertTrue(result.isPresent());
        assertEquals(expectedValue, result.get().asText());
    }

    @Test
    void getValueAtGivenPath_WithNonExistentPath_ShouldReturnEmpty() {
        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(
                jsonObject, "nonexistent/path");

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void getValueAtGivenPath_WithNullPath_ShouldReturnEmpty() {
        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(jsonObject, null);

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void getValueAtGivenPath_WithEmptyPath_ShouldReturnEmpty() {
        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(jsonObject, "");

        // Assert
        assertFalse(result.isPresent());
    }

    @ParameterizedTest
    @MethodSource("provideDifferentTypeValues")
    void getValueAtGivenPath_WithDifferentTypes_ShouldReturnCorrectValue(Object value, String expectedType) {
        // Arrange
        if (value instanceof Integer) {
            jsonObject.put("key", (Integer) value);
        } else if (value instanceof Boolean) {
            jsonObject.put("key", (Boolean) value);
        } else if (value instanceof Double) {
            jsonObject.put("key", (Double) value);
        } else {
            jsonObject.put("key", String.valueOf(value));
        }

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(jsonObject, "key");

        // Assert
        assertTrue(result.isPresent());
        assertEquals(value.toString(), result.get().asText());
    }

    @Test
    void getValueAtGivenPath_WithInvalidIntermediatePath_ShouldReturnEmpty() {
        // Arrange
        jsonObject.put("key", "value");

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(
                jsonObject, "nonexistent/key/child");

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void getValueAtGivenPath_WithNullIntermediateNode_ShouldReturnEmpty() {
        // Arrange
        ObjectNode parentNode = jsonObject.putObject("parent");
        parentNode.putNull("child");

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(
                jsonObject, "parent/child/grandchild");

        // Assert
        assertFalse(result.isPresent());
    }

    @Test
    void getValueAtGivenPath_WithArrayNode_ShouldReturnEmpty() {
        // Arrange
        jsonObject.putArray("array").add("value");

        // Act
        Optional<JsonNode> result = HtmlToTextConversionUtil.getValueAtGivenPath(
                jsonObject, "array/0");

        // Assert
        assertFalse(result.isPresent());
    }
}

