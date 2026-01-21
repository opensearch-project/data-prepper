/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;

/**
 * The plugin to use for unauthenticated access to Armeria servers. It
 * disables authentication on endpoints.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, pluginType = ArmeriaHttpAuthenticationProvider.class)
public class UnauthenticatedArmeriaHttpAuthenticationProvider implements ArmeriaHttpAuthenticationProvider {
}
