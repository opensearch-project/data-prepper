package org.opensearch.dataprepper.plugins.sink.opensearch;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum DistributionVersion {
    ES6("es6"),
    DEFAULT("default");

    private static final Map<String, DistributionVersion> VERSION_MAP = Arrays.stream(DistributionVersion.values())
            .collect(Collectors.toMap(
                    value -> value.version,
                    value -> value
            ));

    private final String version;

    DistributionVersion(final String version) {
        this.version = version;
    }

    public static DistributionVersion fromTypeName(final String version) {
        if (!VERSION_MAP.containsKey(version)) {
            throw new IllegalArgumentException(String.format("Invalid distribution_version value: %s", version));
        }
        return VERSION_MAP.get(version);
    }

    public String getVersion() {
        return version;
    }
}
