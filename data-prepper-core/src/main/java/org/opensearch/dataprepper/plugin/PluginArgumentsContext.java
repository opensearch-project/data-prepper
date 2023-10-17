/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import java.util.Optional;

interface PluginArgumentsContext {
    Object[] createArguments(final Class<?>[] parameterTypes, final Optional<Object> optionalArgument);
}
