package org.opensearch.dataprepper.pipeline;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum OpenSearchAPI {

    BULK_API("bulk"),
    SEARCH_API("search"),
    INDEX_API("index");

    private final String value;

    /**
     * This is a reverse-lookup map for getting a IndexType from a value.
     */
    private static final Map<String, OpenSearchAPI> STRING_TO_OPENSEARCH_API_MAP = new HashMap<>();

    static {
        Arrays.stream(OpenSearchAPI.values())
                .forEach(indexType -> STRING_TO_OPENSEARCH_API_MAP.put(indexType.value, indexType));
    }

    OpenSearchAPI(final String value){
        this.value = value;
    }

    public String getValue(){
        return value;
    }

    /**
     * This is for getting an IndexType enum from a given string value.
     * @param value The string value of an IndexType enum
     * @return IndexType enum matching the string value
     */
    static Optional<OpenSearchAPI> getByValue(final String value) {
        return Optional.ofNullable(STRING_TO_OPENSEARCH_API_MAP.get(value));
    }

    /**
     * This flattens all values into a string which can be used for logging or showing what values are supported
     * for the index_type parameter
     * @return a string containing all values that are supported for the index_type parameter
     */
    static String getOpenSearchAPIValues() {
        return Arrays.stream(OpenSearchAPI.values())
                .map(OpenSearchAPI::getValue)
                .collect(Collectors.toList())
                .toString();
    }
}
