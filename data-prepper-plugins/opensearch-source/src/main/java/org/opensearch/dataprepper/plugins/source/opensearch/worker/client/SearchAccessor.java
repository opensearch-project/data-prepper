/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.NoSearchContextSearchRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;

/**
 * Search Accessor can be used to perform searchs against search clusters.
 *
 * @since 2.4
 */
public interface SearchAccessor {
    /**
     * Information on whether how this SearchAccessor should be used by {@link org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchService}
     * @return the {@link SearchContextType} that has information on which search strategy should be used
     */
    SearchContextType getSearchContextType();

    /**
     * Creates a Point-In-Time (PIT) context for searching
     * @param createPointInTimeRequest - The {@link CreatePointInTimeRequest} details
     * @return {@link CreatePointInTimeResponse} the result of the PIT creation
     * @since 2.4
     */
    CreatePointInTimeResponse createPit(CreatePointInTimeRequest createPointInTimeRequest);

    /**
     * Searches using a PIT context
     * @param searchPointInTimeRequest payload for searching with PIT context
     * @return
     * @since 2.4
     */
    SearchWithSearchAfterResults searchWithPit(SearchPointInTimeRequest searchPointInTimeRequest);

    /**
     * Deletes PITs
     * @param deletePointInTimeRequest The information on which point in time to delete
     * @since 2.4
     */
    void deletePit(DeletePointInTimeRequest deletePointInTimeRequest);

    /**
     * Creates scroll context
     * @param createScrollRequest payload for creating the scroll context
     * @return
     * @since 2.4
     */
    CreateScrollResponse createScroll(CreateScrollRequest createScrollRequest);

    /**
     * Search with scroll context.
     * @param searchScrollRequest payload for searching with scroll context
     * @return
     */
    SearchScrollResponse searchWithScroll(SearchScrollRequest searchScrollRequest);

    /**
     * Deletes scroll contexts
     * @param deleteScrollRequest payload for deleting scroll contexts
     */
    void deleteScroll(DeleteScrollRequest deleteScrollRequest);

    /**
     * Searches with sort and search_after without using any search contexts (Point-in-Time or Scroll)
     */
    SearchWithSearchAfterResults searchWithoutSearchContext(NoSearchContextSearchRequest noSearchContextSearchRequest);
}
