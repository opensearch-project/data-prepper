/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

class CompressionConverter {
    static CompressionCodecName convertCodec(final CompressionOption compressionOption) {
        switch (compressionOption) {
            case NONE:
                return CompressionCodecName.UNCOMPRESSED;
            case GZIP:
                return CompressionCodecName.GZIP;
            case SNAPPY:
                return CompressionCodecName.SNAPPY;
        }

        throw new InvalidPluginDefinitionException(
                String.format("The Parquet codec supports the following compression options: %s, %s, %s",
                        CompressionOption.NONE.getOption(),
                        CompressionOption.GZIP.getOption(),
                        CompressionOption.SNAPPY.getOption()
                ));
    }
}
