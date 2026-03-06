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

public class ConfigDefaults {
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    public static final int DEFAULT_PORT = 21891;
    public static final int DEFAULT_THREAD_COUNT = 200;
    public static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    public static final boolean DEFAULT_SSL = true;
    public static final boolean DEFAULT_ENABLED_UNFRAMED_REQUESTS = false;
    public static final boolean DEFAULT_HEALTH_CHECK = false;
    public static final boolean DEFAULT_PROTO_REFLECTION_SERVICE = false;
    public static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    public static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;
}
