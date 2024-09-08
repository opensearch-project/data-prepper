/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * The interface to implement to become an extension.
 *
 * @since 2.3
 */
public interface ExtensionPlugin {
    /**
     * Register your extension with the available {@link ExtensionPoints} provided
     * by Data Prepper.
     * <p>
     * Each extension will have this method called once on start-up.
     *
     * @param extensionPoints The {@link ExtensionPoints} wherein the extension can extend behaviors.
     */
    void apply(ExtensionPoints extensionPoints);

    /**
     * Close resources used by the extension.
     */
    void shutdown();
}
