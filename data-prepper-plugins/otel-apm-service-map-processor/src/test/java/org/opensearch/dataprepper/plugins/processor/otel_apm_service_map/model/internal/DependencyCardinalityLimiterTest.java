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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter.OTHER_REMOTE_OPERATION;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter.OTHER_REMOTE_SERVICE;

class DependencyCardinalityLimiterTest {

    private final AtomicInteger dependencyOverflows = new AtomicInteger();
    private final AtomicInteger operationOverflows = new AtomicInteger();

    private DependencyCardinalityLimiter newLimiter(final int maxDependencies, final int maxOperations) {
        return new DependencyCardinalityLimiter(maxDependencies, maxOperations,
                dependencyOverflows::incrementAndGet, operationOverflows::incrementAndGet);
    }

    private DependencyCardinalityLimiter admitted(final int maxDependencies, final int maxOperations,
                                                         final String... dependencyCalls) {
        final DependencyCardinalityLimiter limiter = newLimiter(maxDependencies, maxOperations);
        for (final String call : dependencyCalls) {
            final String[] parts = call.split(" ", 2);
            limiter.record("checkout", parts[0], parts.length > 1 ? parts[1] : null);
        }
        limiter.admit();
        return limiter;
    }

    @Test
    void limitDependency_pastTheCap_returnsOverflowBucketAndCountsIt() {
        final DependencyCardinalityLimiter limiter = admitted(2, 10, "postgresql:a", "postgresql:b", "postgresql:c");

        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
        assertThat(limiter.limitDependency("checkout", "postgresql:b"), equalTo("postgresql:b"));
        assertThat(limiter.limitDependency("checkout", "postgresql:c"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitDependency("checkout", "postgresql:c"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(dependencyOverflows.get(), equalTo(2));
    }

    @Test
    void admit_pastTheCap_admitsTheMostCalledDependenciesWithTiesBrokenByName() {
        final DependencyCardinalityLimiter limiter = admitted(2, 10,
                "postgresql:z", "postgresql:c", "postgresql:z", "postgresql:b", "postgresql:a");

        assertThat(limiter.limitDependency("checkout", "postgresql:z"), equalTo("postgresql:z"));
        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
        assertThat(limiter.limitDependency("checkout", "postgresql:b"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitDependency("checkout", "postgresql:c"), equalTo(OTHER_REMOTE_SERVICE));
    }

    @Test
    void admit_sameCallsInAnyOrder_admitsTheSameDependencies() {
        final List<String> calls = new ArrayList<>(List.of(
                "a", "a", "a", "b", "b", "c", "d", "e", "e", "f"));
        final Set<String> expected = Set.of("a", "b", "e");
        for (int seed = 0; seed < 20; seed++) {
            Collections.shuffle(calls, new Random(seed));
            final DependencyCardinalityLimiter limiter = admitted(3, 10, calls.toArray(new String[0]));

            final Set<String> admittedNames = calls.stream()
                    .map(name -> limiter.limitDependency("checkout", name))
                    .filter(name -> !OTHER_REMOTE_SERVICE.equals(name))
                    .collect(Collectors.toSet());

            assertThat("order " + calls, admittedNames, equalTo(expected));
        }
    }

    @Test
    void limitDependency_countsEachSourceServiceSeparately() {
        final DependencyCardinalityLimiter limiter = newLimiter(1, 10);
        limiter.record("checkout", "postgresql:a", null);
        limiter.record("billing", "postgresql:b", null);
        limiter.admit();

        assertThat(limiter.limitDependency("checkout", "postgresql:a"), equalTo("postgresql:a"));
        assertThat(limiter.limitDependency("billing", "postgresql:b"), equalTo("postgresql:b"));
        assertThat(dependencyOverflows.get(), equalTo(0));
    }

    @Test
    void limitRemoteOperation_pastTheCap_returnsOverflowOperationAndCountsIt() {
        final DependencyCardinalityLimiter limiter = admitted(10, 2, "api GET /a", "db GET /a", "db GET /a", "api GET /b");

        assertThat(limiter.limitRemoteOperation("checkout", "api", "GET /a"), equalTo("GET /a"));
        assertThat(limiter.limitRemoteOperation("checkout", "db", "GET /a"), equalTo("GET /a"));
        assertThat(limiter.limitRemoteOperation("checkout", "api", "GET /b"), equalTo(OTHER_REMOTE_OPERATION));
        assertThat(operationOverflows.get(), equalTo(1));
    }

    @Test
    void limitRemoteOperation_nullOperation_isPassedThrough() {
        final DependencyCardinalityLimiter limiter = admitted(10, 1, "api GET /a");

        assertThat(limiter.limitRemoteOperation("checkout", "api", null), equalTo(null));
    }

    @Test
    void limitRemoteOperation_forOverflowedDependency_returnsOverflowOperationWithoutConsumingTheBudget() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1, "a SELECT", "a SELECT", "b INSERT");

        assertThat(limiter.limitDependency("checkout", "b"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitRemoteOperation("checkout", "b", "INSERT"), equalTo(OTHER_REMOTE_OPERATION));
        assertThat(limiter.limitRemoteOperation("checkout", "a", "SELECT"), equalTo("SELECT"));
        assertThat(operationOverflows.get(), equalTo(0));
    }

    @Test
    void admit_operationsOfAnOverflowedDependency_leaveTheBudgetToAdmittedDependencies() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1, "a SELECT", "a SELECT", "b INSERT", "b INSERT", "b INSERT");

        assertThat(limiter.limitDependency("checkout", "b"), equalTo("b"));
        assertThat(limiter.limitRemoteOperation("checkout", "b", "INSERT"), equalTo("INSERT"));
        assertThat(limiter.limitDependency("checkout", "a"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitRemoteOperation("checkout", "a", "SELECT"), equalTo(OTHER_REMOTE_OPERATION));
        assertThat(operationOverflows.get(), equalTo(0));
    }

    @Test
    void limitRemoteOperation_dependencyNameWithSeparatorCharacters_keepsItsOperations() {
        final DependencyCardinalityLimiter limiter = newLimiter(1, 1);
        limiter.record("checkout", "svc\u0000x", "GET /a");
        limiter.admit();

        assertThat(limiter.limitDependency("checkout", "svc\u0000x"), equalTo("svc\u0000x"));
        assertThat(limiter.limitRemoteOperation("checkout", "svc\u0000x", "GET /a"), equalTo("GET /a"));
        assertThat(operationOverflows.get(), equalTo(0));
    }

    @Test
    void limitRemoteOperation_forOverflowedDependencyWithoutOperation_returnsOverflowOperation() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1, "a SELECT", "a SELECT", "b");

        assertThat(limiter.limitDependency("checkout", "b"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitRemoteOperation("checkout", "b", null), equalTo(OTHER_REMOTE_OPERATION));
    }

    @Test
    void limitRemoteOperation_admittedDependencyNamedLikeTheOverflowBucket_keepsItsOperations() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1, OTHER_REMOTE_SERVICE + " GET");

        assertThat(limiter.limitDependency("checkout", OTHER_REMOTE_SERVICE), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitRemoteOperation("checkout", OTHER_REMOTE_SERVICE, "GET"), equalTo("GET"));
    }

    @Test
    void limitRemoteOperation_overflowedCallNextToDependencyNamedLikeTheOverflowBucket_isNotAnOperationOverflow() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1,
                OTHER_REMOTE_SERVICE + " GET", OTHER_REMOTE_SERVICE + " GET", "b INSERT");

        assertThat(limiter.limitDependency("checkout", "b"), equalTo(OTHER_REMOTE_SERVICE));
        assertThat(limiter.limitRemoteOperation("checkout", "b", "INSERT"), equalTo(OTHER_REMOTE_OPERATION));
        assertThat(dependencyOverflows.get(), equalTo(1));
        assertThat(operationOverflows.get(), equalTo(0));
    }

    @Test
    void limit_overflowCallbacks_runOnceForEachCollapsedCall() {
        final DependencyCardinalityLimiter limiter = newLimiter(1, 1);
        limiter.record("checkout", "a", "SELECT");
        limiter.record("checkout", "a", "SELECT");
        limiter.record("checkout", "a", "INSERT");
        limiter.record("checkout", "b", "SELECT");
        limiter.admit();

        limiter.limitDependency("checkout", "b");
        limiter.limitDependency("checkout", "b");
        limiter.limitRemoteOperation("checkout", "b", "SELECT");
        limiter.limitRemoteOperation("checkout", "a", "SELECT");
        limiter.limitRemoteOperation("checkout", "a", "INSERT");

        assertThat(dependencyOverflows.get(), equalTo(2));
        assertThat(operationOverflows.get(), equalTo(1));
    }

    @Test
    void limitDependency_beforeAdmit_throws() {
        final DependencyCardinalityLimiter limiter = newLimiter(1, 1);
        limiter.record("checkout", "a", null);

        assertThrows(IllegalStateException.class, () -> limiter.limitDependency("checkout", "a"));
    }

    @Test
    void record_afterAdmit_throws() {
        final DependencyCardinalityLimiter limiter = admitted(1, 1, "a");

        assertThrows(IllegalStateException.class, () -> limiter.record("checkout", "b", null));
    }
}
