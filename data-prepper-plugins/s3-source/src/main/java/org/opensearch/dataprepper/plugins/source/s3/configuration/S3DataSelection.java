/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum S3DataSelection {

    DATA_ONLY("data_only"),
    METADATA_ONLY("metadata_only"),
    DATA_AND_METADATA("data_and_metadata");

    private static final Map<String, S3DataSelection> S3_DATA_SELECTION_MAP = Arrays.stream(S3DataSelection.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    S3DataSelection(final String type) {
        this.type = type;
    }

    @JsonCreator
    public static S3DataSelection fromOptionValue(final String name) {
        return S3_DATA_SELECTION_MAP.get(name);
    }
}

