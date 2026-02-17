/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import java.time.Duration;
import java.util.function.Consumer;

public class InactiveAcknowledgementSetManager implements AcknowledgementSetManager {
    private static InactiveAcknowledgementSetManager theInstance;

    public static InactiveAcknowledgementSetManager getInstance() {
        if (theInstance == null) {
            theInstance = new InactiveAcknowledgementSetManager();
        }
        return theInstance;
    }
    
    public AcknowledgementSet create(final Consumer<Boolean> callback, final Duration timeout) {
        throw new UnsupportedOperationException("create operation not supported");
    }

}
