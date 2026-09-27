/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter.OTHER_REMOTE_OPERATION;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter.OTHER_REMOTE_SERVICE;

class DependencyCardinalityLimiterTest {

    @Test
    void limitDependency_pastTheCap_returnsOverflowBucketAndCountsIt() {
        final DependencyCardinalityLimiter limiter = new DependencyCardinalityLimiter(2, 10);

        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
        assertThat(limiter.limitDependency("checkout", "postgresql:b"), equalTo("postgresql:b"));
        assertThat(limiter.limitDependency("checkout", "postgresql:c"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitDependency("checkout", "postgresql:c"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.getDependencyOverflowCount(), equalTo(2));
    }

    @Test
    void limitDependency_alreadyAdmittedName_staysAdmittedAfterTheCapIsReached() {
        final DependencyCardinalityLimiter limiter = new DependencyCardinalityLimiter(1, 10);

        limiter.limitDependency("checkout", "postgresql:a");
        limiter.limitDependency("checkout", "postgresql:b");

        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
    }

    @Test
    void limitDependency_countsEachSourceServiceSeparately() {
        final DependencyCardinalityLimiter limiter = new DependencyCardinalityLimiter(1, 10);

        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
        assertThat(limiter.limitDependency("billing", "postgresql:b"), equalTo("postgresql:b"));
        assertThat(limiter.getDependencyOverflowCount(), equalTo(0));
    }

    @Test
    void limitRemoteOperation_pastTheCap_returnsOverflowOperationAndCountsIt() {
        final DependencyCardinalityLimiter limiter = new DependencyCardinalityLimiter(10, 2);

        assertThat(limiter.limitRemoteOperation("checkout", "api", "GET /a"), equalTo("GET /a"));
        assertThat(limiter.limitRemoteOperation("checkout", "db", "GET /a"), equalTo("GET /a"));
        assertThat(limiter.limitRemoteOperation("checkout", "api", "GET /b"), equalTo(OTHER_REMOTE_OPERATION));
        assertThat(limiter.limitRemoteOperation("checkout", "api", "GET /a"), equalTo("GET /a"));
        assertThat(limiter.getRemoteOperationOverflowCount(), equalTo(1));
    }

    @Test
    void limitRemoteOperation_nullOperation_isPassedThrough() {
        final DependencyCardinalityLimiter limiter = new DependencyCardinalityLimiter(10, 1);

        limiter.limitRemoteOperation("checkout", "api", "GET /a");

        assertThat(limiter.limitRemoteOperation("checkout", "api", null), equalTo(null));
    }
}
