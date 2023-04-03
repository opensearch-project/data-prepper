/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum S3SelectSerializationFormatOption {
	CSV("csv"),
	JSON("json"),
	PARQUET("parquet");
	
	private final String option;
	 
    private static final Map<String, S3SelectSerializationFormatOption> OPTIONS_MAP = Arrays.stream(S3SelectSerializationFormatOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    S3SelectSerializationFormatOption(final String option) {
        this.option = option;
    }

    @JsonCreator
    static S3SelectSerializationFormatOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
