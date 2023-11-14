/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.time.Instant;
import java.util.function.BiConsumer;

public interface EventHandle {
    /**
     * releases event handle
     *
     * @param result result to be used while releasing. This indicates if
     *               the operation on the event handle is success or not
     * @since 2.2
     */
    void release(boolean result);

    /**
     * sets external origination time
     *
     * @param externalOriginationTime externalOriginationTime to be set in the event handle
     * @since 2.6
     */
    void setExternalOriginationTime(final Instant externalOriginationTime);

    /**
     * gets external origination time
     *
     * @return returns externalOriginationTime from the event handle. This can be null if it is never set.
     * @since 2.6
     */
    Instant getExternalOriginationTime();

    /**
     * gets internal origination time
     *
     * @return returns internalOriginationTime from the event handle.
     * @since 2.6
     */
    Instant getInternalOriginationTime();

    /**
     * registers onRelease consumer with event handle
     *
     * @param releaseConsumer consumer to be calledback when event handle is released.
     */
    void onRelease(BiConsumer<EventHandle, Boolean> releaseConsumer);
    
}
