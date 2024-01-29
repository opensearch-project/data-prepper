/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingsHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MappingsHandler.class);
    public List<MappingsParameterConfig> getS3FileMappings(S3ObjectConfig awsConfig, String key) {
        if( !isAwsConfigValid(awsConfig) || key == null ) {
            return null;
        }
        String regionString = awsConfig.getRegion();
        Region region = Region.of(regionString);
        String roleARN = awsConfig.getStsRoleArn();
        String bucketName = awsConfig.getBucket();

        List<MappingsParameterConfig> s3FileMappings;
        String roleSessionName = "translate-session";
        try {
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(region)
                    .build();

            AssumeRoleResponse response = stsClient.assumeRole(AssumeRoleRequest.builder()
                    .roleArn(roleARN)
                    .roleSessionName(roleSessionName)
                    .build());
            Credentials temporaryCredentials = response.credentials();
            AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                    temporaryCredentials.accessKeyId(),
                    temporaryCredentials.secretAccessKey(),
                    temporaryCredentials.sessionToken());
            S3Client s3Client = S3Client.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(sessionCredentials))
                    .region(region)
                    .build();
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            try {
                // Retrieve the S3 object
                ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);

                // Read the content of the S3 object
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = responseInputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                }

                // Close the responseInputStream
                responseInputStream.close();

                // Convert the ByteArrayOutputStream to byte array
                byte[] fileData = byteArrayOutputStream.toByteArray();

                // Process the byte array (e.g., convert to mappings)
                s3FileMappings = getMappingsFromByteArray(fileData);

                // Close the ByteArrayOutputStream
                byteArrayOutputStream.close();
            } catch (IOException | AwsServiceException e) {
                LOG.error("Error while retrieving mappings from S3 Object", e);
                return null;
            }
        } catch (AwsServiceException e) {
            LOG.error("Error while retrieving mappings from S3 Object", e);
            return null;
        }
        return s3FileMappings;
    }

    public List<MappingsParameterConfig> getMappingsFromFilePath(String fileName){
        try{
            Path filePath = Paths.get(fileName);
            byte[] fileData = Files.readAllBytes(filePath);
            return getMappingsFromByteArray(fileData);
        }catch (IOException ex){
            LOG.error("Unable to parse the mappings from file", ex);
            return null;
        }
    }

    private List<MappingsParameterConfig> getMappingsFromByteArray(byte[] file){
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            FileMappingsRef fileParser = mapper.readValue(file, FileMappingsRef.class);
            return fileParser.getFileMappingConfigs();
        } catch (IOException ex) {
            LOG.error("Unable to parse the mappings from file", ex);
            return null;
        }
    }

    public List<MappingsParameterConfig> getCombinedMappings(List<MappingsParameterConfig> mappingConfigs, List<MappingsParameterConfig> fileMappingConfigs) {
        if(Objects.isNull(mappingConfigs) || mappingConfigs.isEmpty()){
            return fileMappingConfigs;
        }
        if(fileMappingConfigs == null){
            return mappingConfigs;
        }
        try{
            for (MappingsParameterConfig fileMappingConfig : fileMappingConfigs) {
                boolean isDuplicateSource = false;
                for (MappingsParameterConfig mappingConfig : mappingConfigs) {
                    if (mappingConfig.getSource().equals(fileMappingConfig.getSource())) {
                        isDuplicateSource = true;
                        combineTargets(fileMappingConfig, mappingConfig);
                    }
                }
                if (!isDuplicateSource) {
                    mappingConfigs.add(fileMappingConfig);
                }
            }
            return mappingConfigs;
        } catch (Exception ex){
            Logger LOG = LoggerFactory.getLogger(TranslateProcessor.class);
            LOG.error("Error while combining mappings", ex);
            return null;
        }
    }

    private void combineTargets(MappingsParameterConfig filePathMapping, MappingsParameterConfig mappingConfig) {
        if(Objects.isNull(mappingConfig)) {
            return;
        }
        List<TargetsParameterConfig> fileTargetConfigs = filePathMapping.getTargetsParameterConfigs();
        List<TargetsParameterConfig> mappingsTargetConfigs = mappingConfig.getTargetsParameterConfigs();
        List<TargetsParameterConfig> combinedTargetConfigs = new ArrayList<>(mappingsTargetConfigs);

        for (TargetsParameterConfig fileTargetConfig : fileTargetConfigs) {
            if (!isTargetPresent(fileTargetConfig, combinedTargetConfigs)) {
                combinedTargetConfigs.add(fileTargetConfig);
            }
        }
        mappingConfig.setTargetsParameterConfigs(combinedTargetConfigs);
    }

    private boolean isTargetPresent(TargetsParameterConfig fileTargetConfig, List<TargetsParameterConfig> combinedTargetConfigs){
        String fileTarget = fileTargetConfig.getTarget();
        return combinedTargetConfigs.stream().anyMatch(targetConfig -> fileTarget.equals(targetConfig.getTarget()));
    }

    private boolean isAwsConfigValid(S3ObjectConfig awsConfig) {
        if (awsConfig != null
            && awsConfig.getBucket() != null
            && awsConfig.getRegion() != null
            && awsConfig.getStsRoleArn() != null) {
            return true;
        }
        return false;
    }

}
