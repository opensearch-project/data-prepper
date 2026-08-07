/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
public class DefaultProgressCheck implements ProgressCheck {
    double ratio;

    public DefaultProgressCheck(double ratio) {
        this.ratio = ratio;
    }

    @Override
    public Double getRatio() {
        return ratio;
    }
}
