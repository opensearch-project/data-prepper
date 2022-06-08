/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

public class CompressionCodec implements Codec {

    private final Codec innerCodec;
    private final CompressionOption compressionOption;

    public CompressionCodec(Codec innerCodec, CompressionOption compressionOption) {

    }

    void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        if(compressionOption == CompressionOption.GZIP) {
            innerCodec.parse(new GZIPInputStream(inputStream), eventConsumer);
        } else if(compressionOption == CompressionOption.AUTOMATIC) {
            ResponseInputStream<GetObjectResponse> s3InputStream = (ResponseInputStream<GetObjectResponse>) inputStream;

            s3InputStream.response()
        }

    }
}
