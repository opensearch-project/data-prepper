/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.AwsAuthenticationOptionsConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3DBServiceTest {

    private static final String S3_URI = "s3://mybucket10012023/GeoLite2/test-database.mmdb";
    private static final String DATABASE_DIR = "blue_database";
    private static final String DATABASE_NAME = "test-database";
    
    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;
    @Mock
    private AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig;
    @Mock
    private S3Client s3Client;
    @Mock
    private S3ClientBuilder s3ClientBuilder;
    @Mock
    private AwsCredentialsProvider credentialsProvider;
    
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of(DATABASE_NAME, S3_URI));
        when(awsAuthenticationOptionsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationOptionsConfig.authenticateAwsConfiguration()).thenReturn(credentialsProvider);
    }

    @Test
    void initiateDownloadTest_DownloadFailedException() {
        S3DBService downloadThroughS3 = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> downloadThroughS3.initiateDownload());
    }

    @Test
    void testOverwriteFunctionality() throws IOException {
        String testDatabaseDir = tempDir.toString();
        
        String initialContent = "Initial database content\nVersion: 1.0";
        String updatedContent = "Updated database content\nVersion: 2.0";
        
        File destinationFile = new File(testDatabaseDir + File.separator + DATABASE_NAME + ".mmdb");
        Files.createDirectories(destinationFile.getParentFile().toPath());
        Files.write(destinationFile.toPath(), initialContent.getBytes(StandardCharsets.UTF_8));
        
        String readContent = Files.readString(destinationFile.toPath());
        assertEquals(initialContent, readContent);
        
        try (MockedStatic<S3Client> s3ClientMockedStatic = mockStatic(S3Client.class)) {
            when(s3ClientBuilder.region(any(Region.class))).thenReturn(s3ClientBuilder);
            when(s3ClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(s3ClientBuilder);
            when(s3ClientBuilder.build()).thenReturn(s3Client);
            s3ClientMockedStatic.when(S3Client::builder).thenReturn(s3ClientBuilder);
            
            ArgumentCaptor<Consumer<GetObjectRequest.Builder>> requestCaptor = ArgumentCaptor.forClass(Consumer.class);
            ArgumentCaptor<ResponseTransformer<GetObjectResponse, ?>> transformerCaptor = ArgumentCaptor.forClass(ResponseTransformer.class);
            
            when(s3Client.getObject(requestCaptor.capture(), transformerCaptor.capture())).thenAnswer(invocation -> {
                Files.write(destinationFile.toPath(), updatedContent.getBytes(StandardCharsets.UTF_8));
                return null;
            });
            
            S3DBService s3DBService = createObjectUnderTest();
            s3DBService.initiateDownload();
        }
        String finalContent = Files.readString(destinationFile.toPath());
        assertEquals(updatedContent, finalContent);
        verify(s3Client, times(1)).getObject(any(Consumer.class), any(ResponseTransformer.class));
        
    }
    
    private S3DBService createObjectUnderTest() {
        return new S3DBService(awsAuthenticationOptionsConfig, DATABASE_DIR, maxMindDatabaseConfig);
    }
}
