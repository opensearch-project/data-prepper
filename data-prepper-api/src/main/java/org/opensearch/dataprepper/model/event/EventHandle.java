/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

public interface EventHandle {
    /**
     * releases event handle
     *
     * @param result result to be used while releasing. This indicates if
     *               the operation on the event handle is success or not
     * @since 2.2
     */
    void release(boolean result);
}
