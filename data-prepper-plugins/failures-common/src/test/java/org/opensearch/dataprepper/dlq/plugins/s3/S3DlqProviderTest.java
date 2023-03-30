/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.dlq.plugins.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.dlq.DlqWriter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3DlqProviderTest {

    @Mock
    private S3DlqWriterConfig s3DlqWriterConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private S3Client s3Client;

    @Test
    public void testGetDlqWriterReturnsDlqWriter() {
        final S3DlqProvider s3DlqProvider = new S3DlqProvider(s3DlqWriterConfig, pluginMetrics);
        when(s3DlqWriterConfig.getS3Client()).thenReturn(s3Client);
        when(s3DlqWriterConfig.getBucket()).thenReturn(UUID.randomUUID().toString());

        final Optional<DlqWriter> result = s3DlqProvider.getDlqWriter();
        assertThat(result, notNullValue());
        assertThat(result.isPresent(), is(equalTo(true)));
    }

    @Test
    public void testGetDlqWriterWithInvalidConfigThrowsException() {
        assertThrows(NullPointerException.class, () -> new S3DlqProvider(null, pluginMetrics));
    }
}
