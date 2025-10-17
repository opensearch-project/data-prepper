/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.version;

import org.opensearch.dataprepper.model.configuration.VersionProvider;

public class TestVersionProvider implements VersionProvider {
    @Override
    public String getVersionString() {
        return "2";
    }
}
