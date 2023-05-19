/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import java.io.IOException;

public interface SearchAPICalls {

      void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffer) throws IOException;

      void searchPitIndexes( final String pitID ,final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer);

      void getScrollResponse(final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffers);


}