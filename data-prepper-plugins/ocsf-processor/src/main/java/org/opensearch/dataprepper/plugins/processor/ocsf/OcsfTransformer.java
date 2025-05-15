/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import org.opensearch.dataprepper.model.event.Event;

public interface OcsfTransformer {
    void transform(Event event, final String version) throws Exception;
}
