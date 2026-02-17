/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

public interface PeerForwarderCodec {
    byte[] serialize(PeerForwardingEvents peerForwardingEvents) throws Exception;

    PeerForwardingEvents deserialize(byte[] bytes) throws Exception;
}
