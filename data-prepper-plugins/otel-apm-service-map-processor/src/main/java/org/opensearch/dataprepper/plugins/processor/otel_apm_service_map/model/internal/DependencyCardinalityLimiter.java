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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bounds, for one evaluation window, the distinct dependency names and dependency remote operations
 * each source service may emit. Values past a cap collapse into a fixed overflow bucket.
 * <p>
 * Every call of the window is first recorded, then {@link #admit()} admits, per source service, the
 * values with the most calls (ties broken by name). The admitted set therefore depends only on the
 * window's calls, not on the order they are processed in. Windows are independent.
 */
public class DependencyCardinalityLimiter {
    public static final String OTHER_REMOTE_SERVICE = "OtherRemoteService";
    public static final String OTHER_REMOTE_OPERATION = "OtherRemoteOperation";

    private static final Comparator<String> NAME_ORDER = Comparator.nullsFirst(Comparator.naturalOrder());
    private static final Comparator<OperationKey> OPERATION_KEY_ORDER = Comparator
            .comparing((OperationKey key) -> key.dependencyName, NAME_ORDER)
            .thenComparing(key -> key.remoteOperation);

    private final int maxDependenciesPerService;
    private final int maxRemoteOperationsPerService;
    private final Map<String, Map<String, Integer>> dependencyCallsBySource = new HashMap<>();
    private final Map<String, Map<OperationKey, Integer>> remoteOperationCallsBySource = new HashMap<>();
    private final Map<String, Set<String>> dependenciesBySource = new HashMap<>();
    private final Map<String, Set<OperationKey>> remoteOperationsBySource = new HashMap<>();
    private final Runnable onDependencyOverflow;
    private final Runnable onRemoteOperationOverflow;
    private boolean admitted;

    /**
     * @param onDependencyOverflow      Run once for each call collapsed into {@link #OTHER_REMOTE_SERVICE}
     * @param onRemoteOperationOverflow Run once for each call of an admitted dependency collapsed into
     *                                  {@link #OTHER_REMOTE_OPERATION}
     */
    public DependencyCardinalityLimiter(final int maxDependenciesPerService, final int maxRemoteOperationsPerService,
                                        final Runnable onDependencyOverflow, final Runnable onRemoteOperationOverflow) {
        this.maxDependenciesPerService = maxDependenciesPerService;
        this.maxRemoteOperationsPerService = maxRemoteOperationsPerService;
        this.onDependencyOverflow = onDependencyOverflow;
        this.onRemoteOperationOverflow = onRemoteOperationOverflow;
    }

    /**
     * Records one dependency call of the window. Must be called before {@link #admit()}.
     *
     * @param sourceKey       Identity of the calling service (environment and name)
     * @param dependencyName  The dependency name
     * @param remoteOperation The remote operation, may be null
     */
    public void record(final String sourceKey, final String dependencyName, final String remoteOperation) {
        if (admitted) {
            throw new IllegalStateException("Calls cannot be recorded after admission");
        }
        dependencyCallsBySource.computeIfAbsent(sourceKey, k -> new HashMap<>()).merge(dependencyName, 1, Integer::sum);
        if (remoteOperation != null) {
            remoteOperationCallsBySource.computeIfAbsent(sourceKey, k -> new HashMap<>())
                    .merge(new OperationKey(dependencyName, remoteOperation), 1, Integer::sum);
        }
    }

    /**
     * Admits, per source service, the most-called dependencies and then the most-called remote operations
     * of those admitted dependencies. Operations of an overflowed dependency never use the operation budget.
     */
    public void admit() {
        dependencyCallsBySource.forEach((sourceKey, calls) ->
                dependenciesBySource.put(sourceKey, mostCalled(calls, NAME_ORDER, maxDependenciesPerService)));
        remoteOperationCallsBySource.forEach((sourceKey, calls) -> {
            final Set<String> admittedDependencies = dependenciesBySource.getOrDefault(sourceKey, Collections.emptySet());
            final Map<OperationKey, Integer> admittedDependencyCalls = calls.entrySet().stream()
                    .filter(entry -> admittedDependencies.contains(entry.getKey().dependencyName))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            remoteOperationsBySource.put(sourceKey,
                    mostCalled(admittedDependencyCalls, OPERATION_KEY_ORDER, maxRemoteOperationsPerService));
        });
        admitted = true;
    }

    /**
     * @param sourceKey      Identity of the calling service (environment and name)
     * @param dependencyName The dependency name
     * @return The dependency name, or {@link #OTHER_REMOTE_SERVICE} when it was not admitted
     */
    public String limitDependency(final String sourceKey, final String dependencyName) {
        if (isAdmitted(dependenciesBySource, sourceKey, dependencyName)) {
            return dependencyName;
        }
        onDependencyOverflow.run();
        return OTHER_REMOTE_SERVICE;
    }

    /**
     * @param sourceKey       Identity of the calling service (environment and name)
     * @param dependencyName  The dependency name as recorded, before {@link #limitDependency} is applied, so an
     *                        overflowed call is never mistaken for a dependency that is really named
     *                        {@link #OTHER_REMOTE_SERVICE}
     * @param remoteOperation The remote operation, may be null
     * @return The remote operation (null when it is null and the dependency was admitted), or
     * {@link #OTHER_REMOTE_OPERATION} when it was not admitted or its dependency overflowed. Only the
     * former counts as a remote operation overflow; the latter is already counted as a dependency overflow.
     */
    public String limitRemoteOperation(final String sourceKey, final String dependencyName, final String remoteOperation) {
        if (!isAdmitted(dependenciesBySource, sourceKey, dependencyName)) {
            return OTHER_REMOTE_OPERATION;
        }
        if (remoteOperation == null) {
            return null;
        }
        if (isAdmitted(remoteOperationsBySource, sourceKey, new OperationKey(dependencyName, remoteOperation))) {
            return remoteOperation;
        }
        onRemoteOperationOverflow.run();
        return OTHER_REMOTE_OPERATION;
    }

    private <T> boolean isAdmitted(final Map<String, Set<T>> admittedBySource, final String sourceKey, final T value) {
        if (!admitted) {
            throw new IllegalStateException("Calls must be admitted before they are limited");
        }
        return admittedBySource.getOrDefault(sourceKey, Collections.emptySet()).contains(value);
    }

    private <T> Set<T> mostCalled(final Map<T, Integer> calls, final Comparator<T> tieBreak, final int cap) {
        return calls.entrySet().stream()
                .sorted(Map.Entry.<T, Integer>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey(tieBreak)))
                .limit(cap)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private static final class OperationKey {
        private final String dependencyName;
        private final String remoteOperation;

        private OperationKey(final String dependencyName, final String remoteOperation) {
            this.dependencyName = dependencyName;
            this.remoteOperation = remoteOperation;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OperationKey)) {
                return false;
            }
            final OperationKey that = (OperationKey) o;
            return Objects.equals(dependencyName, that.dependencyName) && remoteOperation.equals(that.remoteOperation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dependencyName, remoteOperation);
        }
    }
}
