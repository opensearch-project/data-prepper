/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
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

public class ElasticsearchAccessor implements SearchAccessor, ClusterClientFactory {
    @Override
    public SearchContextType getSearchContextType() {
        // todo: implement
        return null;
    }

    @Override
    public CreatePointInTimeResponse createPit(final CreatePointInTimeRequest createPointInTimeRequest) {
        //todo: implement
        return null;
    }

    @Override
    public SearchWithSearchAfterResults searchWithPit(SearchPointInTimeRequest searchPointInTimeRequest) {
        //todo: implement
        return null;
    }

    @Override
    public void deletePit(final DeletePointInTimeRequest deletePointInTimeRequest) {
        //todo: implement
    }

    @Override
    public CreateScrollResponse createScroll(CreateScrollRequest createScrollRequest) {
        //todo: implement
        return null;
    }

    @Override
    public SearchScrollResponse searchWithScroll(SearchScrollRequest searchScrollRequest) {
        //todo: implement
        return null;
    }

    @Override
    public void deleteScroll(DeleteScrollRequest deleteScrollRequest) {
        //todo: implement
    }

    @Override
    public SearchWithSearchAfterResults searchWithoutSearchContext(NoSearchContextSearchRequest noSearchContextSearchRequest) {
        return null;
    }

    @Override
    public Object getClient() {
        return null;
    }
}
