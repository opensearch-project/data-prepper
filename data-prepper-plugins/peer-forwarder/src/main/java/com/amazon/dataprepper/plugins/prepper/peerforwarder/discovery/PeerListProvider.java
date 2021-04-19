/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.linecorp.armeria.client.Endpoint;
import com.linecorp.armeria.common.util.Listenable;

import java.util.List;

/**
 * Provides a list other Data Prepper instance endpoints within the same cluster.
 * Coupling this to Armeria's Listenable interface for now to leverage existing implementers of it
 * (see DynamicEndpointGroup), though this can be redefined in the future if more Provider implementations are necessary.
 */
public interface PeerListProvider extends Listenable<List<Endpoint>> {
    String PEER_ENDPOINTS = "peerEndpoints";

    List<String> getPeerList();
}
