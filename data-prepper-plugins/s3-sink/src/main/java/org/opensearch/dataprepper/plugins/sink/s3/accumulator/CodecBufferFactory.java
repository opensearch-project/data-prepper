package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.codec.BufferedCodec;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.function.Supplier;

public class CodecBufferFactory implements BufferFactory {
    private final BufferFactory innerBufferFactory;
    private final BufferedCodec bufferedCodec;

    public CodecBufferFactory(BufferFactory innerBufferFactory, BufferedCodec codec) {
        this.innerBufferFactory = innerBufferFactory;
        this.bufferedCodec = codec;
    }

    @Override
    public Buffer getBuffer(S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier) {
        Buffer innerBuffer = innerBufferFactory.getBuffer(s3Client, bucketSupplier, keySupplier);
        return new CodecBuffer(innerBuffer, bufferedCodec);
    }
}
