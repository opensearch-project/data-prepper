package org.opensearch.dataprepper.plugins.sink.opensearch;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum BackendVersion {
    ES6("es_6");

    private static final Map<String, BackendVersion> VERSION_MAP = Arrays.stream(BackendVersion.values())
            .collect(Collectors.toMap(
                    value -> value.version,
                    value -> value
            ));

    private final String version;

    BackendVersion(final String version) {
        this.version = version;
    }

    public static BackendVersion fromTypeName(final String version) {
        return VERSION_MAP.get(version);
    }
}
