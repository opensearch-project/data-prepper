/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.Experimental;

public class TestPluginWithExperimentalFeatureConfiguration {

    @Experimental
    private String experimentalOption;

    public String getExperimentalOption() {
        return experimentalOption;
    }
}
