/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq;


import org.opensearch.dataprepper.model.failures.DlqObject;

import java.io.IOException;
import java.util.List;

/**
 * An interface for writing DLQ objects to the DLQ
 *
 * @since 2.2
 */
public interface DlqWriter {

    /**
     * Writes the DLQ objects to the DLQ
     * @param dlqObjects the list of objects to be written to the DLQ
     * @param pipelineName the pipeline the DLQ object is associated with.
     * @param pluginId the id of the plugin the DLQ object is associated with.
     * @throws IOException io exception
     *
     * @since 2.2
     */
    void write(final List<DlqObject> dlqObjects, final String pipelineName, final String pluginId) throws IOException;

    /**
     * Closes any open connections to the DLQ
     * @throws IOException io exception
     *
     * @since 2.2
     */
    void close() throws IOException;
}
