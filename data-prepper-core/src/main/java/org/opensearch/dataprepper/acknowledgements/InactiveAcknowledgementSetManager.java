/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import java.util.function.Consumer;
import java.time.Duration;

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

    public void acquireEventReference(final Event event) {
        throw new UnsupportedOperationException("acquire operation not supported");
    }
    
    public void acquireEventReference(final EventHandle eventHandle) {
        throw new UnsupportedOperationException("acquire operation not supported");
    }
    
    public void releaseEventReference(final EventHandle eventHandle, boolean success) {
        throw new UnsupportedOperationException("release operation not supported");
    }
}
