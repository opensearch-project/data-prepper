/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NavigableSet;
import java.util.Optional;

import org.opensearch.dataprepper.plugins.sink.S3ObjectIndex;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.S3SinkService;
import org.opensearch.dataprepper.plugins.sink.SinkAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

/**
 * 
 * @author de20436406
 *
 */
public class LocalFileAccumulator implements SinkAccumulator {
	private static final Logger LOG = LoggerFactory.getLogger(LocalFileAccumulator.class);
	private static final int MAX_RETRY = 3;
	private final S3SinkService s3SinkService;
	private final S3SinkConfig s3SinkConfig;

	private final NavigableSet<String> localFileEventSet;
	private String fileAbsolutePath = null;
	private String localFileName = null;

	/**
	 * 
	 * @param localFileEventSet
	 * @param s3SinkService
	 * @param s3SinkConfig
	 */
	public LocalFileAccumulator(final NavigableSet<String> localFileEventSet, final S3SinkService s3SinkService,
			final S3SinkConfig s3SinkConfig) {
		this.localFileEventSet = localFileEventSet;
		this.s3SinkService = s3SinkService;
		this.s3SinkConfig = s3SinkConfig;
	}

	@Override
	public void doAccumulate() {
		boolean retry = Boolean.FALSE;
		localFileName = S3ObjectIndex.getIndexAliasWithDate(s3SinkConfig.getObjectOptions().getFilePattern());
		File file = new File(localFileName);
		int retryCount = MAX_RETRY;

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(localFileName))) {
			for (String event : localFileEventSet) {
				writer.write(event);
			}
			fileAbsolutePath = file.getAbsoluteFile().toString();
			writer.flush();
			LOG.info("data stored in local file {}", fileAbsolutePath);
			do {
				retry = !fileSaveToS3();
				if (retryCount == 0) {
					retry = Boolean.FALSE;
					LOG.warn("Maximum retry count 3 reached, Unable to store {} into Amazon S3", localFileName);
				}
				if (retry) {
					LOG.info("Retry : {}", (MAX_RETRY - --retryCount));
					Thread.sleep(5000);
				}
			} while (retry);
		} catch (IOException e) {
			LOG.error("Events unable to save into local file : {}", localFileName, e);
		} catch (Exception e) {
			LOG.error("Unable to store {} into Amazon S3", localFileName, e);
		} finally {
			try {
				boolean isLocalFileDeleted = Files.deleteIfExists(Paths.get(fileAbsolutePath));
				if (isLocalFileDeleted) {
					LOG.info("Local file deleted successfully {}", fileAbsolutePath);
				} else {
					LOG.warn("Local file not deleted {}", fileAbsolutePath);
				}
			} catch (IOException e) {
				LOG.error("Local file unable to deleted {}", fileAbsolutePath);
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("finally")
	private boolean fileSaveToS3() {
		final String bucketName = s3SinkConfig.getBucketName();
		final String path = s3SinkConfig.getKeyPathPrefix();
		final String key = path + "/" + localFileName;
		boolean isFileSaveToS3 = Boolean.FALSE;
		try {
			S3Client client = s3SinkService.getS3Client();
			PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).acl("public-read")
					.build();
			client.putObject(request, RequestBody.fromFile(new File(fileAbsolutePath)));
			S3Waiter waiter = client.waiter();
			HeadObjectRequest requestWait = HeadObjectRequest.builder().bucket(bucketName).key(key).build();
			WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
			Optional<HeadObjectResponse> response = waiterResponse.matched().response();
			isFileSaveToS3 = response.isPresent();
		} catch (AwsServiceException | SdkClientException e) {
			LOG.error("Amazon s3 client Exception : ", e);
		} finally {
			if (isFileSaveToS3) {
				LOG.info("File {} was uploaded..... Success !!", localFileName);
			} else {
				LOG.info("File {} was uploaded..... Failed !!", localFileName);
			}
			return isFileSaveToS3;
		}
	}
}
