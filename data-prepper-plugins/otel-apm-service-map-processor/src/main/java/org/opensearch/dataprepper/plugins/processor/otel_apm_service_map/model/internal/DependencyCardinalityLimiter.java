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
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Bounds the distinct dependency names and dependency remote operations each source service may emit.
 * Values past a cap collapse into a fixed overflow bucket.
 * <p>
 * Admission is sticky across windows. Every call of a window is first recorded, then {@link #admit()}
 * keeps the values the source already holds (so an admitted dependency does not switch to an overflow
 * name in the next window) and fills any free slots with the most-called new values (ties broken by
 * name, so the result does not depend on processing order). A value not seen for more than
 * {@code idleWindowsBeforeEviction} windows is evicted, freeing its slot. Call {@link #beginWindow()}
 * before recording each window.
 */
public class DependencyCardinalityLimiter {
    /** Fallback overflow name for a dependency whose node type is unknown. */
    public static final String OTHER_REMOTE_SERVICE = "OtherRemoteService";
    /** Overflow name for database dependencies. */
    public static final String OTHER_DATABASE = "OtherDatabase";
    /** Overflow name for external dependencies. */
    public static final String OTHER_EXTERNAL = "OtherExternal";
    /** Overflow name for messaging (broker) dependencies. */
    public static final String OTHER_MESSAGING = "OtherMessaging";
    /** Overflow name for remote operations. */
    public static final String OTHER_REMOTE_OPERATION = "OtherRemoteOperation";
    /** Windows a value may go unseen before its slot is released. */
    public static final int DEFAULT_IDLE_WINDOWS_BEFORE_EVICTION = 10;

    private static final Comparator<String> NAME_ORDER = Comparator.nullsFirst(Comparator.naturalOrder());
    private static final Comparator<OperationKey> OPERATION_KEY_ORDER = Comparator
            .comparing((OperationKey key) -> key.dependencyName, NAME_ORDER)
            .thenComparing(key -> key.remoteOperation);

    private final int maxDependenciesPerService;
    private final int maxRemoteOperationsPerService;
    private final int idleWindowsBeforeEviction;
    private final Runnable onDependencyOverflow;
    private final Runnable onRemoteOperationOverflow;

    /** Sticky state: per source, each admitted dependency and the last window it was seen in. */
    private final Map<String, Map<String, Long>> admittedDependenciesBySource = new HashMap<>();
    /** Sticky state: per source, each admitted remote operation and the last window it was seen in. */
    private final Map<String, Map<OperationKey, Long>> admittedOperationsBySource = new HashMap<>();
    /** Calls recorded in the current window, per source and dependency. */
    private final Map<String, Map<String, Integer>> dependencyCallsBySource = new HashMap<>();
    /** Calls recorded in the current window, per source and (dependency, remote operation). */
    private final Map<String, Map<OperationKey, Integer>> remoteOperationCallsBySource = new HashMap<>();
    /** Index of the current window, used for idle eviction. */
    private long window;
    /** Whether {@link #admit()} has run for the current window. */
    private boolean admitted;

    /**
     * @param onDependencyOverflow      Run once for each call collapsed into a dependency overflow name
     * @param onRemoteOperationOverflow Run once for each call of an admitted dependency collapsed into
     *                                  {@link #OTHER_REMOTE_OPERATION}
     */
    public DependencyCardinalityLimiter(final int maxDependenciesPerService, final int maxRemoteOperationsPerService,
                                        final Runnable onDependencyOverflow, final Runnable onRemoteOperationOverflow) {
        this(maxDependenciesPerService, maxRemoteOperationsPerService, DEFAULT_IDLE_WINDOWS_BEFORE_EVICTION,
                onDependencyOverflow, onRemoteOperationOverflow);
    }

    /**
     * @param idleWindowsBeforeEviction Windows a value may go unseen before its slot is released
     * @param onDependencyOverflow      Run once for each call collapsed into a dependency overflow name
     * @param onRemoteOperationOverflow Run once for each call of an admitted dependency collapsed into
     *                                  {@link #OTHER_REMOTE_OPERATION}
     */
    public DependencyCardinalityLimiter(final int maxDependenciesPerService, final int maxRemoteOperationsPerService,
                                        final int idleWindowsBeforeEviction,
                                        final Runnable onDependencyOverflow, final Runnable onRemoteOperationOverflow) {
        this.maxDependenciesPerService = maxDependenciesPerService;
        this.maxRemoteOperationsPerService = maxRemoteOperationsPerService;
        this.idleWindowsBeforeEviction = idleWindowsBeforeEviction;
        this.onDependencyOverflow = onDependencyOverflow;
        this.onRemoteOperationOverflow = onRemoteOperationOverflow;
    }

    /**
     * @param nodeType The dependency node type (database / external / messaging), may be null
     * @return The overflow name for that type, so overflow nodes of different types never share a name
     */
    public static String overflowNameFor(final String nodeType) {
        if (SpanStateData.NODE_TYPE_DATABASE.equals(nodeType)) {
            return OTHER_DATABASE;
        }
        if (SpanStateData.NODE_TYPE_EXTERNAL.equals(nodeType)) {
            return OTHER_EXTERNAL;
        }
        if (SpanStateData.NODE_TYPE_MESSAGING.equals(nodeType)) {
            return OTHER_MESSAGING;
        }
        return OTHER_REMOTE_SERVICE;
    }

    /**
     * Starts a new window: clears the previous window's recorded calls and advances the window counter
     * used for idle eviction. Admitted values carry over.
     */
    public void beginWindow() {
        dependencyCallsBySource.clear();
        remoteOperationCallsBySource.clear();
        window++;
        admitted = false;
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
            throw new IllegalStateException("Calls cannot be recorded after admission; call beginWindow() first");
        }
        dependencyCallsBySource.computeIfAbsent(sourceKey, k -> new HashMap<>()).merge(dependencyName, 1, Integer::sum);
        if (remoteOperation != null) {
            remoteOperationCallsBySource.computeIfAbsent(sourceKey, k -> new HashMap<>())
                    .merge(new OperationKey(dependencyName, remoteOperation), 1, Integer::sum);
        }
    }

    /**
     * Admits, per source service, the dependencies it already holds plus the most-called new ones up to the
     * cap, then the remote operations of the admitted dependencies the same way. Operations of an
     * overflowed dependency never use the operation budget. Idle values are evicted first.
     */
    public void admit() {
        evictIdle(admittedDependenciesBySource);
        evictIdle(admittedOperationsBySource);
        dependencyCallsBySource.forEach((sourceKey, calls) -> admitSticky(
                admittedDependenciesBySource.computeIfAbsent(sourceKey, k -> new HashMap<>()),
                calls, NAME_ORDER, maxDependenciesPerService));
        remoteOperationCallsBySource.forEach((sourceKey, calls) -> {
            final Map<String, Long> admittedDependencies =
                    admittedDependenciesBySource.getOrDefault(sourceKey, Collections.emptyMap());
            final Map<OperationKey, Integer> admittedDependencyCalls = calls.entrySet().stream()
                    .filter(entry -> admittedDependencies.containsKey(entry.getKey().dependencyName))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            admitSticky(admittedOperationsBySource.computeIfAbsent(sourceKey, k -> new HashMap<>()),
                    admittedDependencyCalls, OPERATION_KEY_ORDER, maxRemoteOperationsPerService);
        });
        admittedDependenciesBySource.values().removeIf(Map::isEmpty);
        admittedOperationsBySource.values().removeIf(Map::isEmpty);
        admitted = true;
    }

    /**
     * @param sourceKey      Identity of the calling service (environment and name)
     * @param dependencyName The dependency name
     * @return The dependency name, or {@link #OTHER_REMOTE_SERVICE} when it was not admitted
     */
    public String limitDependency(final String sourceKey, final String dependencyName) {
        return limitDependency(sourceKey, dependencyName, null);
    }

    /**
     * @param sourceKey      Identity of the calling service (environment and name)
     * @param dependencyName The dependency name
     * @param nodeType       The dependency node type, which selects the overflow name
     * @return The dependency name, or the type's overflow name (see {@link #overflowNameFor}) when it was
     * not admitted
     */
    public String limitDependency(final String sourceKey, final String dependencyName, final String nodeType) {
        if (isAdmitted(admittedDependenciesBySource, sourceKey, dependencyName)) {
            return dependencyName;
        }
        onDependencyOverflow.run();
        return overflowNameFor(nodeType);
    }

    /**
     * @param sourceKey       Identity of the calling service (environment and name)
     * @param dependencyName  The dependency name as recorded, before {@link #limitDependency} is applied, so an
     *                        overflowed call is never mistaken for a dependency that is really named like an
     *                        overflow bucket
     * @param remoteOperation The remote operation, may be null
     * @return The remote operation (null when it is null and the dependency was admitted), or
     * {@link #OTHER_REMOTE_OPERATION} when it was not admitted or its dependency overflowed. Only the
     * former counts as a remote operation overflow; the latter is already counted as a dependency overflow.
     */
    public String limitRemoteOperation(final String sourceKey, final String dependencyName, final String remoteOperation) {
        if (!isAdmitted(admittedDependenciesBySource, sourceKey, dependencyName)) {
            return OTHER_REMOTE_OPERATION;
        }
        if (remoteOperation == null) {
            return null;
        }
        if (isAdmitted(admittedOperationsBySource, sourceKey, new OperationKey(dependencyName, remoteOperation))) {
            return remoteOperation;
        }
        onRemoteOperationOverflow.run();
        return OTHER_REMOTE_OPERATION;
    }

    /**
     * @param sourceKey Identity of the calling service (environment and name)
     * @return The number of dependencies currently held for the source
     */
    int admittedDependencyCount(final String sourceKey) {
        return admittedDependenciesBySource.getOrDefault(sourceKey, Collections.emptyMap()).size();
    }

    private <T> void admitSticky(final Map<T, Long> held, final Map<T, Integer> calls,
                                 final Comparator<T> tieBreak, final int cap) {
        // Values already held stay admitted; refresh the ones seen in this window.
        calls.keySet().forEach(value -> held.computeIfPresent(value, (k, lastSeen) -> window));
        final int free = cap - held.size();
        if (free <= 0) {
            return;
        }
        calls.entrySet().stream()
                .filter(entry -> !held.containsKey(entry.getKey()))
                .sorted(Map.Entry.<T, Integer>comparingByValue(Comparator.reverseOrder())
                        .thenComparing(Map.Entry.comparingByKey(tieBreak)))
                .limit(free)
                .forEach(entry -> held.put(entry.getKey(), window));
    }

    private <T> void evictIdle(final Map<String, Map<T, Long>> heldBySource) {
        for (final Map<T, Long> held : heldBySource.values()) {
            final Iterator<Map.Entry<T, Long>> it = held.entrySet().iterator();
            while (it.hasNext()) {
                if (window - it.next().getValue() > idleWindowsBeforeEviction) {
                    it.remove();
                }
            }
        }
        heldBySource.values().removeIf(Map::isEmpty);
    }

    private <T> boolean isAdmitted(final Map<String, Map<T, Long>> heldBySource, final String sourceKey, final T value) {
        if (!admitted) {
            throw new IllegalStateException("Calls must be admitted before they are limited");
        }
        return heldBySource.getOrDefault(sourceKey, Collections.emptyMap()).containsKey(value);
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
