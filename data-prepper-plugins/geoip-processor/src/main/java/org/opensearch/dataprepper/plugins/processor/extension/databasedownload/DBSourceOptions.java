/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DBSourceOptions enum
 */
public enum DBSourceOptions {
    PATH("path"),
    URL("url"),
    S3("s3"),
    HTTP_MANIFEST("http_manifest");

    private final String option;

    private static final Map<String, DBSourceOptions> OPTIONS_MAP = Arrays.stream(DBSourceOptions.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    DBSourceOptions(final String option) {
        this.option = option;
    }

    @JsonCreator
    static DBSourceOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
