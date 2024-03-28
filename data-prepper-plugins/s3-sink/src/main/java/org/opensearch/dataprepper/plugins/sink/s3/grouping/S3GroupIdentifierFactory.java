/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.KeyGenerator;

public class S3GroupIdentifierFactory {

    private final KeyGenerator keyGenerator;

    public S3GroupIdentifierFactory(final KeyGenerator keyGenerator) {
        this.keyGenerator = keyGenerator;
     }


    public S3GroupIdentifier getS3GroupIdentifierForEvent(final Event event) {

        final String groupIdentificationHash = keyGenerator.generateKeyForEvent(event, false);
        final String fullObjectKey = keyGenerator.generateKeyForEvent(event, true);

        return new S3GroupIdentifier(groupIdentificationHash, fullObjectKey);
    }
}
