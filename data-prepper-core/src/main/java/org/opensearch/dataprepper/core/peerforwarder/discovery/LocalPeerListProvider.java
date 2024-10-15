/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class LocalPeerListProvider implements PeerListProvider {

    static LocalPeerListProvider createPeerListProvider(final PeerForwarderConfiguration peerForwarderConfiguration, final PluginMetrics pluginMetrics) {
        return new LocalPeerListProvider();
    }

    @Override
    public List<String> getPeerList() {
        return Collections.emptyList();
    }

    @Override
    public void addListener(Consumer<? super List<Endpoint>> listener) {
        // Do nothing
    }

    @Override
    public void removeListener(Consumer<?> listener) {
        // Do nothing
    }
}
