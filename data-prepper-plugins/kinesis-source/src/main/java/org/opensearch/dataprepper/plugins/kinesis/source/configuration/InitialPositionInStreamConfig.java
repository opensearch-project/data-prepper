/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import lombok.Getter;
import software.amazon.kinesis.common.InitialPositionInStream;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public enum InitialPositionInStreamConfig {
    LATEST("latest", InitialPositionInStream.LATEST),
    EARLIEST("earliest", InitialPositionInStream.TRIM_HORIZON);

    private final String position;

    private final InitialPositionInStream positionInStream;

    InitialPositionInStreamConfig(final String position, final InitialPositionInStream positionInStream) {
        this.position = position;
        this.positionInStream = positionInStream;
    }

    private static final Map<String, InitialPositionInStreamConfig> POSITIONS_MAP = Arrays.stream(InitialPositionInStreamConfig.values())
            .collect(Collectors.toMap(
                    value -> value.position,
                    value -> value
            ));

    public static InitialPositionInStreamConfig fromPositionValue(final String position) {
        return POSITIONS_MAP.get(position.toLowerCase());
    }

    public String toString() {
        return this.position;
    }
}
