/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

public interface HasByteDecoder {
  default ByteDecoder getDecoder() {
    return null;
  }
}
