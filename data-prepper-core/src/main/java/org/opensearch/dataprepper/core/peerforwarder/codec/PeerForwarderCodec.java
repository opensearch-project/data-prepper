/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

public interface PeerForwarderCodec {
    byte[] serialize(PeerForwardingEvents peerForwardingEvents) throws Exception;

    PeerForwardingEvents deserialize(byte[] bytes) throws Exception;
}
