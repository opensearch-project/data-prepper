package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.codec.BufferedCodec;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.Function;

public class CodecBufferFactory implements BufferFactory {
    private final BufferFactory innerBufferFactory;
    private final BufferedCodec bufferedCodec;

    public CodecBufferFactory(BufferFactory innerBufferFactory, BufferedCodec codec) {
        this.innerBufferFactory = innerBufferFactory;
        this.bufferedCodec = codec;
    }

    @Override
    public Buffer getBuffer(final S3AsyncClient s3Client,
                            final Supplier<String> bucketSupplier,
                            final Supplier<String> keySupplier,
                            final String defaultBucket,
                            final Function<Integer, Map<String, String>> metadataSupplier,
                            final BucketOwnerProvider bucketOwnerProvider) {
        Buffer innerBuffer = innerBufferFactory.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket, metadataSupplier, bucketOwnerProvider);
        return new CodecBuffer(innerBuffer, bufferedCodec);
    }
}
