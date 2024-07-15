/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class S3ObjectReaderTest {

    @Mock
    private S3Client s3Client;

    private S3ObjectReader s3ObjectReader;


    @BeforeEach
    void setUp() {
        s3ObjectReader = createObjectUnderTest();
    }

    @Test
    void test_readFile() {
        final String bucketName = UUID.randomUUID().toString();
        final String key = UUID.randomUUID().toString();


        s3ObjectReader.readFile(bucketName, key);

        ArgumentCaptor<GetObjectRequest> getObjectRequestArgumentCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(getObjectRequestArgumentCaptor.capture());

        GetObjectRequest request = getObjectRequestArgumentCaptor.getValue();
        assertThat(request.bucket(), equalTo(bucketName));
        assertThat(request.key(), equalTo(key));
    }

    private S3ObjectReader createObjectUnderTest() {
        return new S3ObjectReader(s3Client);
    }
}
