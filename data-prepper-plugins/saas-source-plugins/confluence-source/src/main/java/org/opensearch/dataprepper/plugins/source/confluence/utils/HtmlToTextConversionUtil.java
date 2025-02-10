package org.opensearch.dataprepper.plugins.source.confluence.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.Optional;

public class HtmlToTextConversionUtil {

    public static JsonNode convertHtmlToText(ObjectNode jsonObject, String path) {
        Optional<JsonNode> valueAtGivenPath = getValueAtGivenPath(jsonObject, path);
        if (valueAtGivenPath.isPresent()) {
            String html = valueAtGivenPath.get().textValue();
            String txtBody = convertHtmlToText(html);
            setValueAtGivenPath(jsonObject, path, txtBody);
        }
        return jsonObject;
    }

    public static void setValueAtGivenPath(ObjectNode jsonObject, String path, String value) {
        String[] keys = path.split("/");
        JsonNode current = jsonObject;
        for (int i = 0; i < keys.length - 1; i++) {
            current = current.get(keys[i]);
        }
        ((ObjectNode) current).put(keys[keys.length - 1], value);
    }

    public static Optional<JsonNode> getValueAtGivenPath(ObjectNode jsonObject, String path) {
        try {
            String[] keys = path.split("/");
            ObjectNode current = jsonObject;
            for (int i = 0; i < keys.length - 1; i++) {
                current = (ObjectNode) current.get(keys[i]);
            }
            return Optional.of(current.get(keys[keys.length - 1]));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static String convertHtmlToText(String html) {
        if (html == null || html.isEmpty()) {
            return "";
        }
        Document document = Jsoup.parse(html);
        // Remove scripts and style elements
        document.select("script, style").remove();
        return document.text();
    }
}
