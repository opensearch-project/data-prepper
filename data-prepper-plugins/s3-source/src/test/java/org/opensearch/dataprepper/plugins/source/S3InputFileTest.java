package org.opensearch.dataprepper.plugins.source;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3InputFileTest {

    private S3Client s3Client;
    private S3ObjectReference s3ObjectReference;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private S3InputFile s3InputFile;

    @BeforeEach
    public void setUp() {
        s3Client = mock(S3Client.class);
        s3ObjectReference = mock(S3ObjectReference.class);
        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);

        s3InputFile = new S3InputFile(s3Client, s3ObjectReference, s3ObjectPluginMetrics);
    }

    @Test
    public void testGetLength() {
        HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);
        when(headObjectResponse.contentLength()).thenReturn(12345L);

        long length = s3InputFile.getLength();

        assertThat(length, equalTo(12345L));
        verify(s3Client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    @Test
    public void testNewStream() {
        HeadObjectResponse headObjectResponse = mock(HeadObjectResponse.class);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headObjectResponse);

        SeekableInputStream seekableInputStream = s3InputFile.newStream();

        assertThat(seekableInputStream.getClass(), equalTo(S3InputStream.class));
    }

}
