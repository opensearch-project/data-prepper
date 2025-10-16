/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

public class ShardNotTrackedException extends RuntimeException {
    public ShardNotTrackedException(String message) {
      super(message);
    }
}
