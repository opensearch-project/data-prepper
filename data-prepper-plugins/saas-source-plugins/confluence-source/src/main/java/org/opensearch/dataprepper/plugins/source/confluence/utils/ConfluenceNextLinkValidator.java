package org.opensearch.dataprepper.plugins.source.confluence.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConfluenceNextLinkValidator {
    // Define allowed parameters and their patterns
    private static final Map<String, Pattern> ALLOWED_PARAMS = Map.of(
            "next", Pattern.compile("^(true|false)$"),
            "cursor", Pattern.compile("^[A-Za-z0-9+/=_%\\-]+$"),
            "expand", Pattern.compile("^[A-Za-z0-9+/=_%\\-.,]+$"),
            "limit", Pattern.compile("^\\d{1,3}$"),
            "start", Pattern.compile("^\\d+$"),
            "startAt", Pattern.compile("^\\d+$"),
            "maxResults", Pattern.compile("^\\d+$"),
            "cql", Pattern.compile("^[\\w\\s=\"()><%\\-.:]+$")
    );

    public static String validateAndSanitizeURL(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);
        String query = url.getQuery();

        if (query == null || query.isEmpty()) {
            return urlString;
        }

        // Parse and validate parameters
        Map<String, String> validatedParams = new HashMap<>();
        String[] pairs = query.split("&");

        for (String pair : pairs) {

            String key = URLDecoder.decode(pair.substring(0, pair.indexOf("=")), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(pair.indexOf("=") + 1), StandardCharsets.UTF_8);

            // Check if parameter is allowed and matches pattern
            if (ALLOWED_PARAMS.containsKey(key) &&
                    ALLOWED_PARAMS.get(key).matcher(value).matches()) {
                validatedParams.put(key, value);
            }
        }

        // Rebuild URL with validated parameters
        StringBuilder sanitizedURL = new StringBuilder();
        sanitizedURL.append(url.getProtocol()).append("://")
                .append(url.getHost())
                .append(url.getPath())
                .append("?");

        // Add validated parameters
        String params = validatedParams.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" +
                        URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        sanitizedURL.append(params);

        return sanitizedURL.toString();
    }
}

