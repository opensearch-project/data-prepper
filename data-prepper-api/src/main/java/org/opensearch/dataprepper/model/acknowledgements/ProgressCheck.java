/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.acknowledgements;

public interface ProgressCheck {
    /**
     * Returns the pending ratio
     *
     * @return returns the ratio of pending to the total acknowledgements
     * @since 2.6
     */
    Double getRatio();
}

