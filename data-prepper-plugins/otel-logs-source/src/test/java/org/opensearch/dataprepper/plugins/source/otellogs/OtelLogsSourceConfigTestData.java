/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

public class OtelLogsSourceConfigTestData {
    public static final String CONFIG_GRPC_PATH = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";
    public static final String CONFIG_HTTP_PATH = "/logs/v1";
    public static final String BASIC_AUTH_USERNAME = "test";
    public static final String BASIC_AUTH_PASSWORD = "password";
}
