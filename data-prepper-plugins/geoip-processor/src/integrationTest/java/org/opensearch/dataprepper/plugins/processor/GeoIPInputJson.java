/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

public class GeoIPInputJson {
    Peer PeerObject;
    private String status;

    // Getter Methods
    public Peer getPeer() {
        return PeerObject;
    }
    public String getStatus() {
        return status;
    }
    // Setter Methods
    public void setPeer( Peer peerObject ) {
        this.PeerObject = peerObject;
    }
    public void setStatus( String status ) {
        this.status = status;
    }
}
