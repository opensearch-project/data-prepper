package org.opensearch.dataprepper.plugins.sink.s3.codec;

import org.opensearch.dataprepper.model.codec.OutputCodec;

import java.util.Optional;

/**
 * Represents a {@link OutputCodec} which supplies its own buffer.
 */
public interface BufferedCodec extends OutputCodec {
    Optional<Long> getSize();
}
