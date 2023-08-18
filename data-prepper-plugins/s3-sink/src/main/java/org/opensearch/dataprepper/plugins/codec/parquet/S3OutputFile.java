/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;


class S3OutputFile implements OutputFile {
    private final S3OutputStream outputStream;

    public S3OutputFile(S3OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return outputStream;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return null;
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

}
