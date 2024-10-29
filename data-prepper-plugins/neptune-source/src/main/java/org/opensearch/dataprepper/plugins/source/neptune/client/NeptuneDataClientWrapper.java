/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.client;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.neptunedata.NeptunedataClient;
import software.amazon.awssdk.services.neptunedata.model.GetPropertygraphStreamRequest;
import software.amazon.awssdk.services.neptunedata.model.GetPropertygraphStreamResponse;
import software.amazon.awssdk.services.neptunedata.model.GetSparqlStreamRequest;
import software.amazon.awssdk.services.neptunedata.model.GetSparqlStreamResponse;
import software.amazon.awssdk.services.neptunedata.model.IteratorType;

import java.util.List;
import java.util.stream.Collectors;

public class NeptuneDataClientWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(NeptuneDataClientWrapper.class);
    private final NeptunedataClient client;

    @Getter
    private final StreamType streamType;

    @Getter
    private final long batchSize;

    public enum StreamType {
        PROPERTY_GRAPH,
        SPARQL;

        public static StreamType fromString(String name) {
            if (name.equalsIgnoreCase("propertygraph")) {
                return PROPERTY_GRAPH;
            } else if (name.equalsIgnoreCase("sparql")) {
                return SPARQL;
            }
            throw new IllegalArgumentException("Unknown stream type: " + name);
        }
    }

    private NeptuneDataClientWrapper(final NeptuneSourceConfig sourceConfig, final long batchSize) {
        this.client = NeptuneDataClientFactory.provideNeptuneDataClient(sourceConfig);
        this.streamType = StreamType.fromString(sourceConfig.getStreamType());
        this.batchSize = batchSize;
    }

    public static NeptuneDataClientWrapper create(final NeptuneSourceConfig sourceConfig, final long batchSize) {
        return new NeptuneDataClientWrapper(sourceConfig, batchSize);
    }

    private IteratorType getIteratorType(final long checkPointCommitNum,
                                         final long checkPointOpNum) {
        return (checkPointOpNum == 0 && checkPointCommitNum == 0)
                ? IteratorType.TRIM_HORIZON :
                IteratorType.AFTER_SEQUENCE_NUMBER;
    }

    public List<NeptuneStreamRecord> getStreamRecords(final long checkPointCommitNum,
                                                      final long checkPointOpNum) {
        final IteratorType iteratorType = getIteratorType(checkPointCommitNum, checkPointOpNum);
        final List<?> records;
        if (this.streamType == StreamType.PROPERTY_GRAPH) {
            final GetPropertygraphStreamResponse propertyGraphStream =
                    this.getPropertyGraphStream(checkPointCommitNum, checkPointOpNum, iteratorType);
            records = propertyGraphStream.records();
        } else {
            records = this.getSparqlStream(checkPointCommitNum, checkPointOpNum, iteratorType).records();
        }
        return records.stream().map(NeptuneStreamRecord::fromStreamRecord).collect(Collectors.toList());
    }

    public GetPropertygraphStreamResponse getPropertyGraphStream(final long checkPointCommitNum,
                                                                 final long checkPointOpNum,
                                                                 final IteratorType iteratorType) {
        assert this.streamType == StreamType.PROPERTY_GRAPH;
        final GetPropertygraphStreamRequest request =
                GetPropertygraphStreamRequest
                        .builder()
                        .limit(batchSize)
                        .commitNum(iteratorType == IteratorType.TRIM_HORIZON ? null : checkPointCommitNum)
                        .opNum(iteratorType == IteratorType.TRIM_HORIZON ? null : checkPointOpNum)
                        .iteratorType(iteratorType)
                        .build();
        return this.client.getPropertygraphStream(request);
    }

    public GetSparqlStreamResponse getSparqlStream(final long checkPointCommitNum,
                                                   final long checkPointOpNum,
                                                   final IteratorType iteratorType) {
        assert this.streamType == StreamType.SPARQL;
        final GetSparqlStreamRequest request = GetSparqlStreamRequest
                .builder()
                .limit(batchSize)
                .commitNum(checkPointCommitNum)
                .opNum(checkPointOpNum)
                .iteratorType(iteratorType)
                .build();
        return this.client.getSparqlStream(request);
    }

}
