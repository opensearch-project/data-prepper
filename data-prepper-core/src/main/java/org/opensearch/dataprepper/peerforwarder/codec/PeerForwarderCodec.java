/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.codec;

import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;

public interface PeerForwarderCodec {
    byte[] serialize(PeerForwardingEvents peerForwardingEvents) throws Exception;

    PeerForwardingEvents deserialize(byte[] bytes) throws Exception;
}
