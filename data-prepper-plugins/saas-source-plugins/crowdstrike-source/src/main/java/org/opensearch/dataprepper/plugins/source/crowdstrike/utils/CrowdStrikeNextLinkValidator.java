package org.opensearch.dataprepper.plugins.source.crowdstrike.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.FILTER_KEY;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.LAST_UPDATED;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.LIMIT_KEY;
import static org.opensearch.dataprepper.plugins.source.crowdstrike.utils.Constants.MARKER_KEY;

/**
 * Utility class for validating and sanitizing CrowdStrike API URLs.
 */
public class CrowdStrikeNextLinkValidator {

    private static final Logger log = LoggerFactory.getLogger(CrowdStrikeNextLinkValidator.class);

    private static final Set<String> ALLOWED_FILTER_KEYS = Set.of(LAST_UPDATED, MARKER_KEY);
    private static final Pattern FILTER_COMPONENT_PATTERN =
            Pattern.compile("^([a-z_]+):(>=|<=|>|<)['%a-zA-Z0-9:\\-.]+$");
    private static final Pattern LIMIT_PATTERN = Pattern.compile("^\\d{1,5}$");

    /**
     * Validates and sanitizes a CrowdStrike API URL, preserving only allowed query parameters.
     * For the 'filter' parameter, only 'last_updated' and '_marker' components are retained.
     *
     * @param urlString the full API URL to validate
     * @return sanitized version of the URL
     * @throws MalformedURLException if the URL is malformed
     */
    public static String validateAndSanitizeURL(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);
        String query = url.getQuery();

        if (query == null || query.isEmpty()) {
            return urlString;
        }

        Map<String, String> validatedParams = new HashMap<>();
        String[] pairs = query.split("&");

        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx <= 0) continue;

            String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);

            if (FILTER_KEY.equals(key)) {
                List<String> validSubFilters = new ArrayList<>();

                String encodedValue = pair.substring(idx + 1); // This is still URL-encoded
                String[] encodedSubFilters = encodedValue.split("%2B"); // Use raw '%2B' which is '+' in URL encoding

                for (String encodedSub : encodedSubFilters) {
                    String sub;
                    try {
                        sub = URLDecoder.decode(encodedSub, StandardCharsets.UTF_8);
                    } catch (IllegalArgumentException e) {
                        log.warn("Invalid URL encoding in subfilter: {}", encodedSub);
                        continue;
                    }

                    Matcher matcher = FILTER_COMPONENT_PATTERN.matcher(sub);
                    if (matcher.matches()) {
                        String filterKey = sub.substring(0, sub.indexOf(":"));
                        if (ALLOWED_FILTER_KEYS.contains(filterKey)) {
                            validSubFilters.add(sub);
                        } else {
                            log.warn("Disallowed filter key: {}", filterKey);
                        }
                    } else {
                        log.warn("Malformed filter segment: {}", sub);
                    }
                }

                if (!validSubFilters.isEmpty()) {
                    validatedParams.put(FILTER_KEY, String.join("+", validSubFilters));
                }

            } else {
                String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                if (LIMIT_KEY.equals(key) && LIMIT_PATTERN.matcher(value).matches()) {
                    validatedParams.put(LIMIT_KEY, value);
                } else {
                    log.warn("Skipping disallowed or malformed param: {}={}", key, value);
                }
            }
        }
        StringBuilder sanitizedURL = new StringBuilder()
                .append(url.getProtocol()).append("://").append(url.getHost());

        if (url.getPort() != -1) {
            sanitizedURL.append(":").append(url.getPort());
        }

        sanitizedURL.append(url.getPath());

        if (!validatedParams.isEmpty()) {
            String queryString = validatedParams.entrySet().stream()
                    .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "=" +
                            URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                    .collect(Collectors.joining("&"));
            sanitizedURL.append("?").append(queryString);
        }

        return sanitizedURL.toString();
    }

}
