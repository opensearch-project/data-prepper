/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.function.LongSupplier;

/**
 * Operator-facing metrics for the azure_federated OAUTHBEARER token lifecycle. The success counter
 * and the refresh-headroom gauge are registered in the constructor; the per-cause failure counters are
 * registered on first use (the registry dedupes by name+tags). This keeps
 * {@link AzureFederatedTokenProvider} free of any direct Micrometer usage (mirroring the pattern
 * used by KafkaTopicConsumerMetrics). These are internal/operator signals only - they are not
 * vended to customers.
 */
public class KafkaSourceAuthMetrics {
    static final String TOKEN_REFRESH_SUCCESS = "tokenRefreshSuccess";
    static final String TOKEN_REFRESH_FAILURES = "tokenRefreshFailures";
    static final String TIME_TO_TOKEN_REFRESH = "timeToTokenRefresh";
    static final String ERROR_TYPE_TAG = "errorType";

    // Failure cause values (bounded, low cardinality). The token provider already distinguishes these.
    public static final String CAUSE_AWS_STS_ACCESS_DENIED = "aws_sts_access_denied";
    public static final String CAUSE_AWS_STS_ERROR = "aws_sts_error";
    public static final String CAUSE_AWS_OUTBOUND_FEDERATION_DISABLED = "aws_outbound_federation_disabled";
    public static final String CAUSE_AZURE_TOKEN_EXCHANGE_REJECTED = "azure_token_exchange_rejected";
    public static final String CAUSE_NETWORK = "network";
    public static final String CAUSE_OTHER = "other";

    private final PluginMetrics pluginMetrics;
    private final Counter tokenRefreshSuccess;
    private final LongSupplier clock;

    // Absolute epoch millis of the refresh deadline; 0 = no token minted yet. Written on the refresh
    // thread, read on the scrape thread, hence volatile.
    private volatile long refreshDeadlineMs = 0L;

    public KafkaSourceAuthMetrics(final PluginMetrics pluginMetrics) {
        this(pluginMetrics, System::currentTimeMillis);
    }

    KafkaSourceAuthMetrics(final PluginMetrics pluginMetrics, final LongSupplier clock) {
        this.pluginMetrics = pluginMetrics;
        this.clock = clock;
        this.tokenRefreshSuccess = pluginMetrics.counter(TOKEN_REFRESH_SUCCESS);
        // Headroom is computed at scrape time so it counts down between refreshes. The gauge holds only
        // a weak ref to this object, so KafkaSourceAuthMetricsProvider must keep it strongly referenced.
        pluginMetrics.gauge(TIME_TO_TOKEN_REFRESH, this, KafkaSourceAuthMetrics::currentRefreshHeadroomSeconds);
    }

    double currentRefreshHeadroomSeconds() {
        final long deadline = refreshDeadlineMs;
        if (deadline == 0L) {
            return 0.0;
        }
        return Math.max(0.0, (deadline - clock.getAsLong()) / 1000.0);
    }

    /**
     * Record a successful token (re)mint. {@code refreshDeadlineMs} is the absolute epoch-millis
     * instant at which the provider will proactively re-mint (token expiry minus the skew buffer);
     * the timeToTokenRefresh gauge reports headroom to this deadline.
     */
    public void recordRefresh(final long refreshDeadlineMs) {
        this.refreshDeadlineMs = refreshDeadlineMs;
        tokenRefreshSuccess.increment();
    }

    /**
     * Record a failed token (re)mint, tagged by {@code cause} (one of the {@code CAUSE_*} constants)
     * so an operator can route by remediation path (role policy vs Azure app registration vs network).
     * The registry dedupes by name+tags, so repeated causes resolve to the same counter.
     */
    public void recordFailure(final String cause) {
        pluginMetrics.counterWithTags(TOKEN_REFRESH_FAILURES, ERROR_TYPE_TAG, cause).increment();
    }
}
