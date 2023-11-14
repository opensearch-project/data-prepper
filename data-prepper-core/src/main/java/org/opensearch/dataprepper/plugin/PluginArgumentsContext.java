/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

interface PluginArgumentsContext {
    Object[] createArguments(final Class<?>[] parameterTypes, final Object ... args);
}
