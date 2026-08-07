/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

