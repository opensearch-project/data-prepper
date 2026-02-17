/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.sun.net.httpserver.Authenticator;
import org.opensearch.dataprepper.core.pipeline.server.DataPrepperCoreAuthenticationProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;

/**
 * The plugin for unauthenticated core Data Prepper APIs.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
        pluginType = DataPrepperCoreAuthenticationProvider.class)
public class UnauthenticatedDataPrepperCoreAuthenticationProvider implements DataPrepperCoreAuthenticationProvider {
    @Override
    public Authenticator getAuthenticator() {
        return null;
    }
}
