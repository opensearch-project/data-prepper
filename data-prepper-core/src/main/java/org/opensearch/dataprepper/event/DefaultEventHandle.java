/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import java.lang.ref.WeakReference;

public class DefaultEventHandle implements EventHandle {
    private final WeakReference<AcknowledgementSet> acknowledgementSetRef;
    public DefaultEventHandle(AcknowledgementSet acknowledgementSet) {
        this.acknowledgementSetRef = new WeakReference<>(acknowledgementSet);
    }

    public AcknowledgementSet getAcknowledgementSet() {
        return acknowledgementSetRef.get();
    }

    @Override
    public void release(boolean result) {
        AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
        if (acknowledgementSet != null) {
            acknowledgementSet.release(this, result);
        }
    }
}
