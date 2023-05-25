/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.opensearch.dataprepper.plugins.source.opensearch.service.ElasticSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.HostsService;
import org.opensearch.dataprepper.plugins.source.opensearch.service.OpenSearchService;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchTimerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 *  Service that will call both worker classes
 */
public class OpenSearchSourceService {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSourceService.class);

    private final OpenSearchService openSearchService;

    private final ElasticSearchService elasticSearchService;

    private final HostsService hostsService;

    private final OpenSearchSourceConfiguration sourceConfig;

    private final BufferAccumulator bufferAccumulator;

    private final Timer timer = new Timer();

    public OpenSearchSourceService(final OpenSearchSourceConfiguration sourceConfig,
                                   final HostsService hostsService,
                                   final OpenSearchService openSearchService,
                                   final ElasticSearchService elasticSearchService,
                                   final BufferAccumulator bufferAccumulator){
        this.sourceConfig = sourceConfig;
        this.hostsService = hostsService;
        this.openSearchService = openSearchService;
        this.elasticSearchService =elasticSearchService;
        this.bufferAccumulator = bufferAccumulator;
    }

    public void processHosts(){
        sourceConfig.getHosts().forEach(host ->{
            final ServiceInfo serviceInfo = hostsService.findServiceDetailsByUrl(host);
            IndexParametersConfiguration index = sourceConfig.getIndexParametersConfiguration();
            timer.scheduleAtFixedRate(new OpenSearchTimerWorker(openSearchService,elasticSearchService,
                            sourceConfig,bufferAccumulator,serviceInfo,host),
                    sourceConfig.getSchedulingParameterConfiguration().getStartTime().getSecond(),
                    sourceConfig.getSchedulingParameterConfiguration().getRate().toMillis());
        });
    }
    public void stop(){
        timer.cancel();
    }
}
