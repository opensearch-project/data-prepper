package org.opensearch.dataprepper.plugins.source.neptune.stream;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamCheckpoint;

@Getter
@Setter
public class CheckpointStatus {
    private final StreamCheckpoint checkpoint;
    private AcknowledgmentStatus acknowledgeStatus;
    private final long createTimestamp;
    private Long acknowledgedTimestamp;

    enum AcknowledgmentStatus {
        POSITIVE_ACK,
        NEGATIVE_ACK,
        NO_ACK
    }

    public CheckpointStatus(final StreamCheckpoint checkpoint, final long createTimestamp) {
        this.checkpoint = checkpoint;
        this.acknowledgeStatus = AcknowledgmentStatus.NO_ACK;
        this.createTimestamp = createTimestamp;
    }

    public boolean isPositiveAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.POSITIVE_ACK;
    }

    public boolean isNegativeAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.NEGATIVE_ACK;
    }
}
