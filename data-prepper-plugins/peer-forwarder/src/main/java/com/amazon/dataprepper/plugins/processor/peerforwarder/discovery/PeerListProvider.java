package com.amazon.dataprepper.plugins.processor.peerforwarder.discovery;

import java.util.List;

/**
 * Provides a list other Data Prepper instance endpoints within the same cluster.
 */
public interface PeerListProvider {
    List<String> getPeerList();
}
