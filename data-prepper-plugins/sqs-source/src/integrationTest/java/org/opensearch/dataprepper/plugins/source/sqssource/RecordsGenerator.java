/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import java.util.List;

public interface RecordsGenerator {
    void pushMessages(final List<String> messages, final String queueUrl);
}
