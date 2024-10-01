package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

public class StreamEventId {

    private long commitNum;

    private long opNum;

    public long getCommitNum() {
        return commitNum;
    }

    public long getOpNum() {
        return opNum;
    }
}