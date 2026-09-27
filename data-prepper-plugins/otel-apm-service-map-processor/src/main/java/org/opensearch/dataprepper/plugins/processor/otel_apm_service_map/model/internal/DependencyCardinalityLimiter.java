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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bounds, for one evaluation window, the distinct dependency names and dependency remote operations
 * each source service may emit. Values past a cap collapse into a fixed overflow bucket. The first
 * values seen in the window are admitted; a value already admitted stays admitted for the window.
 */
public class DependencyCardinalityLimiter {
    public static final String OTHER_REMOTE_SERVICE = "OtherRemoteService";
    public static final String OTHER_REMOTE_OPERATION = "OtherRemoteOperation";

    private final int maxDependenciesPerService;
    private final int maxRemoteOperationsPerService;
    private final Map<String, Set<String>> dependenciesBySource = new HashMap<>();
    private final Map<String, Set<String>> remoteOperationsBySource = new HashMap<>();
    private int dependencyOverflowCount;
    private int remoteOperationOverflowCount;

    public DependencyCardinalityLimiter(final int maxDependenciesPerService, final int maxRemoteOperationsPerService) {
        this.maxDependenciesPerService = maxDependenciesPerService;
        this.maxRemoteOperationsPerService = maxRemoteOperationsPerService;
    }

    /**
     * @param sourceKey      Identity of the calling service (environment and name)
     * @param dependencyName The dependency name
     * @return The dependency name, or {@link #OTHER_REMOTE_SERVICE} once the source is over its cap
     */
    public String limitDependency(final String sourceKey, final String dependencyName) {
        if (admit(dependenciesBySource, sourceKey, dependencyName, maxDependenciesPerService)) {
            return dependencyName;
        }
        dependencyOverflowCount++;
        return OTHER_REMOTE_SERVICE;
    }

    /**
     * @param sourceKey       Identity of the calling service (environment and name)
     * @param dependencyName  The (already limited) dependency name the operation belongs to
     * @param remoteOperation The remote operation, may be null
     * @return The remote operation, or {@link #OTHER_REMOTE_OPERATION} once the source is over its cap
     */
    public String limitRemoteOperation(final String sourceKey, final String dependencyName, final String remoteOperation) {
        if (remoteOperation == null) {
            return null;
        }
        if (admit(remoteOperationsBySource, sourceKey, dependencyName + "\u0000" + remoteOperation,
                maxRemoteOperationsPerService)) {
            return remoteOperation;
        }
        remoteOperationOverflowCount++;
        return OTHER_REMOTE_OPERATION;
    }

    private static boolean admit(final Map<String, Set<String>> admittedBySource, final String sourceKey,
                                 final String value, final int cap) {
        final Set<String> admitted = admittedBySource.computeIfAbsent(sourceKey, k -> new HashSet<>());
        if (admitted.contains(value)) {
            return true;
        }
        if (admitted.size() < cap) {
            admitted.add(value);
            return true;
        }
        return false;
    }

    public int getDependencyOverflowCount() {
        return dependencyOverflowCount;
    }

    public int getRemoteOperationOverflowCount() {
        return remoteOperationOverflowCount;
    }
}
