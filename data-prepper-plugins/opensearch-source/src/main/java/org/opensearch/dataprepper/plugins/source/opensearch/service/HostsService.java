/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;

/**
 * It takes care of host related data
 */

public class HostsService {

    public ServiceInfo findServiceDetailsByUrl( final String url){
        // if response is not 200 then will call BackoffService for retry
        return new ServiceInfo();
    }

}
