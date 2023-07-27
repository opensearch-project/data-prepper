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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
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
        String clientRegion = awsConfig.getRegion();
        String roleARN = awsConfig.getStsRoleArn();
        String bucketName = awsConfig.getBucket();

        List<MappingsParameterConfig> s3FileMappings;
        String roleSessionName = "translate-session";
        try {
            AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder
                    .standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();
            AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                    .withRoleArn(roleARN)
                    .withRoleSessionName(roleSessionName);
            AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
            Credentials sessionCredentials = roleResponse.getCredentials();
            BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
                    sessionCredentials.getAccessKeyId(),
                    sessionCredentials.getSecretAccessKey(),
                    sessionCredentials.getSessionToken());
            AmazonS3 s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(clientRegion)
                    .build();
            try {
                // Retrieving the S3 object using the bucket name and key.
                S3Object s3Object = s3Client.getObject(bucketName, key);
                S3ObjectInputStream inputStream = s3Object.getObjectContent();

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byte[] read_buf = new byte[1024];
                int read_len = 0;
                while ((read_len = inputStream.read(read_buf)) > 0) {
                    byteArrayOutputStream.write(read_buf, 0, read_len);
                }
                inputStream.close();

                byte[] fileData = byteArrayOutputStream.toByteArray();
                s3FileMappings = getMappingsFromByteArray(fileData);

                byteArrayOutputStream.close();
            } catch (IOException | AmazonServiceException e) {
                LOG.error("Error while retrieving mappings from S3 Object");
                e.printStackTrace();
                return null;
            }
        } catch (AmazonServiceException e) {
            e.printStackTrace();
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
