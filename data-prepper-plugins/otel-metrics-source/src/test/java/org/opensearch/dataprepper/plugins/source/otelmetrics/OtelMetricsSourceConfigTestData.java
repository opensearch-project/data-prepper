/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

public class OtelMetricsSourceConfigTestData {
    public static final String CONFIG_HTTP_PATH = "/metrics/v1";
    public static final String BASIC_AUTH_USERNAME = "test";
    public static final String BASIC_AUTH_PASSWORD = "password";

    public static final String BASE_64_ENCODED_BASIC_AUTH_CREDENTIALS = Base64.getEncoder()
            .encodeToString(String.format("%s:%s", BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD).getBytes(StandardCharsets.UTF_8));

    public static final RetryInfoConfig RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));
}
