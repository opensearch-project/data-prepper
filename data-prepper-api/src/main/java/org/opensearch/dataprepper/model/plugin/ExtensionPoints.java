/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * A model for extending Data Prepper. A Data Prepper extension will call methods in a provided instance
 * of this class.
 *
 * @since 2.3
 */
public interface ExtensionPoints {
    /**
     * Adds an {@link ExtensionProvider} to Data Prepper. This allows an extension to make a class
     * available to plugins within Data Prepper.
     *
     * @param extensionProvider The {@link ExtensionProvider} which this extension is creating.
     * @since 2.3
     */
    void addExtensionProvider(ExtensionProvider<?> extensionProvider);

    <T> T getExtensionProvider(Class<T> type);
}
