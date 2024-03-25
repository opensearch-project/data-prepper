/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.AwsAuthenticationOptionsConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * Implementation class for Download through S3
 */
public class S3DBService implements DBSource {
    private final AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig;
    private final String destinationDirectory;
    private final MaxMindDatabaseConfig maxMindDatabaseConfig;

    /**
     * S3DBService constructor for initialisation of attributes
     *
     * @param destinationDirectory destinationDirectory
     */
    public S3DBService(final AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig,
                       final String destinationDirectory,
                       final MaxMindDatabaseConfig maxMindDatabaseConfig) {
        this.awsAuthenticationOptionsConfig = awsAuthenticationOptionsConfig;
        this.destinationDirectory = destinationDirectory;
        this.maxMindDatabaseConfig = maxMindDatabaseConfig;
    }

    /**
     * Initialisation of Download through Url
     */
    public void initiateDownload() {
        final Set<String> databasePaths = maxMindDatabaseConfig.getDatabasePaths().keySet();

        for (final String database : databasePaths) {
            try {
                final String s3Uri = maxMindDatabaseConfig.getDatabasePaths().get(database);
                final URI uri = new URI(s3Uri);
                final String key = uri.getPath().substring(1);
                final String bucketName = uri.getHost();
                buildRequestAndDownloadFile(bucketName, key, database);
            } catch (URISyntaxException ex) {
                throw new DownloadFailedException("Failed to download database from S3." + ex.getMessage());
            }
        }
    }

    /**
     * Download the mmdb file from the S3
     *
     * @param bucketName Name of the S3 bucket
     * @param key        Name of S3 object key
     * @param fileName   Name of the file to save
     */
    private void buildRequestAndDownloadFile(final String bucketName, final String key, final String fileName) {
        try {
            final S3Client s3Client = createS3Client();

            final File destination = new File(destinationDirectory + File.separator + fileName + MAXMIND_DATABASE_EXTENSION);

            s3Client.getObject(b -> b.bucket(bucketName).key(key), destination.toPath());
        } catch (Exception ex) {
            throw new DownloadFailedException("Failed to download database from S3." + ex.getMessage());
        }
    }

    private S3Client createS3Client() {
        return S3Client.builder()
                .region(awsAuthenticationOptionsConfig.getAwsRegion())
                .credentialsProvider(awsAuthenticationOptionsConfig.authenticateAwsConfiguration())
                .build();
    }
}

