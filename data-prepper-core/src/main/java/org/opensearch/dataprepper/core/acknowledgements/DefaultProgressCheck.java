/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
