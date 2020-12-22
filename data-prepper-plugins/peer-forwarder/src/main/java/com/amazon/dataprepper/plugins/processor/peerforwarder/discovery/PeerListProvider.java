package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.common.util.Listenable;

import java.util.List;

/**
 * Provides a list other Data Prepper instance endpoints within the same cluster.
 * Coupling this to Armeria's Listenable interface for now to leverage existing implementers of it
 * (see DynamicEndpointGroup), though this can be redefined in the future if more Provider implementations are necessary.
 */
public interface PeerListProvider extends Listenable<List<Endpoint>> {
    List<String> getPeerList();
}
