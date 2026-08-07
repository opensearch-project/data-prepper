/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

import java.util.Optional;

/**
 * An interface to be provided by extensions which wish to provide classes to plugins.
 *
 * @param <T> The type of class provided.
 * @since 2.3
 */
public interface ExtensionProvider<T> {
    /**
     * Returns an instance of the class being provided.
     * <p>
     * This is called everytime a plugin requires an instance. The implementor can re-use
     * instances, or create them on-demand depending on the intention of the extension
     * author.
     *
     * @param context The context for the request. This is currently a placeholder.
     * @return An instance as requested.
     */
    Optional<T> provideInstance(Context context);

    /**
     * Returns the Java {@link Class} which this extension is providing.
     *
     * @return A {@link Class}.
     */
    Class<T> supportedClass();

    /**
     * The context for creating a new instance.
     *
     * @since 2.3
     */
    interface Context {

    }
}
