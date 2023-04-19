/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq;

import java.util.Optional;

/**
 * An interface for providing {@link DlqWriter}s.
 * <p>
 * Plugin authors can use this interface for providing {@link DlqWriter}s
 *
 * @since 2.2
 */
public interface DlqProvider {


    /**
     * Allows implementors to provide a {@link DlqWriter}. This may be optional, in which case it is not used.
     * @param pluginMetricsScope the {@link org.opensearch.dataprepper.metrics.PluginMetrics} component scope.
     *                           This is used to place the DLQ metrics under the correct parent plugin.
     * @since 2.2
     */
    default Optional<DlqWriter> getDlqWriter(final String pluginMetricsScope) {
        return Optional.empty();
    }
}
