/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class StreamTransferManager {

	private static final Logger log = LoggerFactory.getLogger(StreamTransferManager.class);
	/** Kilobytes */
	public static final int KB = 1024;
	/** Megabytes */
	public static final int MB = 1024 * KB;

	protected final String bucketName;
	protected final String putKey;
	protected final String path;
	protected final S3Client s3Client;
	protected String uploadId;
	protected int numStreams = 1;
	protected int numUploadThreads = 1;
	protected int queueCapacity = 1;
	protected int partSize = 5 * MB;
	protected boolean checkIntegrity = false;
	private final List<CompletedPart> partETags = Collections.synchronizedList(new ArrayList<CompletedPart>());
	private final List<String> partETagss = Collections.synchronizedList(new ArrayList<String>());
	private List<MultiPartOutputStream> multiPartOutputStreams;
	private ExecutorServiceResultsHandler<Void> executorServiceResultsHandler;
	private ClosableQueue<StreamPart> queue;
	private int finishedCount = 0;
	private StreamPart leftoverStreamPart = null;
	private final Object leftoverStreamPartLock = new Object();
	private boolean isAborting = false;
	private static final int MAX_PART_NUMBER = 10000;

	public StreamTransferManager(String bucketName, String putKey, S3Client s3Client, String path) {
		this.bucketName = bucketName;
		this.putKey = putKey;
		this.s3Client = s3Client;
		this.path = path;
	}

	public StreamTransferManager numStreams(int numStreams) {
		ensureCanSet();
		if (numStreams < 1) {
			throw new IllegalArgumentException("There must be at least one stream");
		}
		this.numStreams = numStreams;
		return this;
	}

	public StreamTransferManager numUploadThreads(int numUploadThreads) {
		ensureCanSet();
		if (numUploadThreads < 1) {
			throw new IllegalArgumentException("There must be at least one upload thread");
		}
		this.numUploadThreads = numUploadThreads;
		return this;
	}

	public StreamTransferManager queueCapacity(int queueCapacity) {
		ensureCanSet();
		if (queueCapacity < 1) {
			throw new IllegalArgumentException("The queue capacity must be at least 1");
		}
		this.queueCapacity = queueCapacity;
		return this;
	}

	public StreamTransferManager partSize(long partSize) {
		ensureCanSet();
		partSize *= MB;
		if (partSize < MultiPartOutputStream.S3_MIN_PART_SIZE) {
			throw new IllegalArgumentException(String.format("The given part size (%d) is less than 5 MB.", partSize));
		}
		if (partSize > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(String
					.format("The given part size (%d) is too large as it does not fit in a 32 bit int", partSize));
		}
		this.partSize = (int) partSize;
		return this;
	}

	public StreamTransferManager checkIntegrity(boolean checkIntegrity) {
		ensureCanSet();
		if (checkIntegrity) {
			Utils.md5(); // check that algorithm is available
		}
		this.checkIntegrity = checkIntegrity;
		return this;
	}

	private void ensureCanSet() {
		if (queue != null) {
			abort();
			throw new IllegalStateException("Setters cannot be called after getMultiPartOutputStreams");
		}

	}

	public List<MultiPartOutputStream> getMultiPartOutputStreams() {
		if (multiPartOutputStreams != null) {
			return multiPartOutputStreams;
		}

		queue = new ClosableQueue<StreamPart>(queueCapacity);
		log.debug("Initiating multipart upload to {}/{}", bucketName, putKey);

		CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
				.bucket(bucketName).key(putKey).build();

		CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
		uploadId = response.uploadId();

		log.info("Initiated multipart upload to {}/{} with full ID {}", bucketName, putKey, uploadId);
		try {
			multiPartOutputStreams = new ArrayList<MultiPartOutputStream>();
			ExecutorService threadPool = Executors.newFixedThreadPool(numUploadThreads);

			int partNumberStart = 1;

			for (int i = 0; i < numStreams; i++) {
				int partNumberEnd = (i + 1) * MAX_PART_NUMBER / numStreams + 1;
				MultiPartOutputStream multiPartOutputStream = new MultiPartOutputStream(partNumberStart, partNumberEnd,
						partSize, queue);
				partNumberStart = partNumberEnd;
				multiPartOutputStreams.add(multiPartOutputStream);
			}

			executorServiceResultsHandler = new ExecutorServiceResultsHandler<Void>(threadPool);
			for (int i = 0; i < numUploadThreads; i++) {
				executorServiceResultsHandler.submit(new UploadTask());
			}
			executorServiceResultsHandler.finishedSubmitting();
		} catch (Exception e) {
			throw abort(e);
		}

		return multiPartOutputStreams;
	}

	//TODO needs to refactor this code
	@SuppressWarnings("static-access")
	public void complete() {
		try {
			log.debug("{}: Waiting for pool termination", this);
			executorServiceResultsHandler.awaitCompletion();
			log.debug("{}: Pool terminated", this);
			if (leftoverStreamPart != null) {
				log.info("{}: Uploading leftover stream {}", this, leftoverStreamPart);
				uploadStreamPart(leftoverStreamPart);
				log.debug("{}: Leftover uploaded", this);
			}
			log.debug("{}: Completing", this);
			
			CompletedMultipartUpload completedMultipartUpload = null;
			
			if(partETags.size() < partSize) {
			switch (partETags.size()) {
			case 1:
				completedMultipartUpload = CompletedMultipartUpload.builder().parts(partETags.get(0)).build();
				break;
			case 2:
				completedMultipartUpload = CompletedMultipartUpload.builder().parts(partETags.get(0), partETags.get(1)).build();
				break;
			case 3:
				completedMultipartUpload = CompletedMultipartUpload.builder().parts(partETags.get(0), partETags.get(1), partETags.get(2)).build();
				break;
			case 4:
				completedMultipartUpload = CompletedMultipartUpload.builder().parts(partETags.get(0), partETags.get(1), partETags.get(2), partETags.get(3)).build();
				break;
			case 5:
				completedMultipartUpload = CompletedMultipartUpload.builder().parts(partETags.get(0), partETags.get(1), partETags.get(2), partETags.get(3), partETags.get(4)).build();
				break;
			default:
				log.error("Part should not be null or empty");
				break;
			}}
			
			//CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(part).build();

			CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
					.bucket(bucketName).key(putKey).uploadId(uploadId).multipartUpload(completedMultipartUpload)
					.build();

			CompleteMultipartUploadResponse completeMultipartUploadResult = s3Client
					.completeMultipartUpload(completeMultipartUploadRequest);

			log.info("{}: Completed", this);
		} catch (IntegrityCheckException e) {
			// Nothing to abort. Upload has already finished.
			throw e;
		} catch (Exception e) {
			throw abort(e);
		}
	}

	/**
	 * Aborts the upload and rethrows the argument, wrapped in a RuntimeException if
	 * necessary. Write {@code throw abort(e)} to make it clear to the compiler and
	 * readers that the code stops here.
	 */
	public RuntimeException abort(Throwable t) {
		if (!isAborting) {
			log.error("Aborting {} due to error: {}", this, t.toString());
		}
		abort();
		if (t instanceof Error) {
			throw (Error) t;

		} else if (t instanceof RuntimeException) {
			throw (RuntimeException) t;

		} else if (t instanceof InterruptedException) {
			throw Utils.runtimeInterruptedException((InterruptedException) t);

		} else {
			throw new RuntimeException(t);
		}
	}

	/**
	 * Aborts the upload. Repeated calls have no effect.
	 */
	public void abort() {
		synchronized (this) {
			if (isAborting) {
				return;
			}
			isAborting = true;
		}
		if (executorServiceResultsHandler != null) {
			executorServiceResultsHandler.abort();
		}
		if (queue != null) {
			queue.close();
		}
		if (uploadId != null) {
			log.debug("{}: Aborting", this);
			/*
			 * AbortMultipartUploadRequest abortMultipartUploadRequest = new
			 * AbortMultipartUploadRequest(bucketName, putKey, uploadId);
			 */

			AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
					.bucket(bucketName).key(putKey).uploadId(uploadId).build();

			s3Client.abortMultipartUpload(abortMultipartUploadRequest);
			log.info("{}: Aborted", this);
		}
	}

	private class UploadTask implements Callable<Void> {

		@Override
		public Void call() {
			try {
				while (true) {
					StreamPart part;
					// noinspection SynchronizeOnNonFinalField
					synchronized (queue) {
						if (finishedCount < multiPartOutputStreams.size()) {
							part = queue.take();
							if (part == StreamPart.POISON) {
								finishedCount++;
								continue;
							}
						} else {
							break;
						}
					}
					if (part.size() < MultiPartOutputStream.S3_MIN_PART_SIZE) {
						/*
						 * Each stream does its best to avoid producing parts smaller than 5 MB, but if
						 * a user doesn't write that much data there's nothing that can be done. These
						 * are considered 'leftover' parts, and must be merged with other leftovers to
						 * try producing a part bigger than 5 MB which can be uploaded without problems.
						 * After the threads have completed there may be at most one leftover part
						 * remaining, which S3 can accept. It is uploaded in the complete() method.
						 */
						log.debug("{}: Received part {} < 5 MB that needs to be handled as 'leftover'", this, part);
						StreamPart originalPart = part;
						part = null;
						synchronized (leftoverStreamPartLock) {
							if (leftoverStreamPart == null) {
								leftoverStreamPart = originalPart;
								log.debug("{}: Created new leftover part {}", this, leftoverStreamPart);
							} else {
								/*
								 * Try to preserve order within the data by appending the part with the higher
								 * number to the part with the lower number. This is not meant to produce a
								 * perfect solution: if the client is producing multiple leftover parts all bets
								 * are off on order.
								 */
								if (leftoverStreamPart.getPartNumber() > originalPart.getPartNumber()) {
									StreamPart temp = originalPart;
									originalPart = leftoverStreamPart;
									leftoverStreamPart = temp;
								}
								leftoverStreamPart.getOutputStream().append(originalPart.getOutputStream());
								log.debug("{}: Merged with existing leftover part to create {}", this,
										leftoverStreamPart);
								if (leftoverStreamPart.size() >= MultiPartOutputStream.S3_MIN_PART_SIZE) {
									log.debug("{}: Leftover part can now be uploaded as normal and reset", this);
									part = leftoverStreamPart;
									leftoverStreamPart = null;
								}
							}
						}
					}
					if (part != null) {
						uploadStreamPart(part);
					}
				}
			} catch (Exception t) {
				throw abort(t);
			}

			return null;
		}

	}

	private void uploadStreamPart(StreamPart streamPart) {
		log.debug("{}: Uploading {}", this, streamPart);

		UploadPartRequest uploadRequest = UploadPartRequest.builder()
				.bucket(bucketName)
				.key(putKey)
				.uploadId(uploadId)
				.partNumber(streamPart.getPartNumber()).build();

		UploadPartResponse response = s3Client.uploadPart(uploadRequest, RequestBody
				.fromInputStream(streamPart.getInputStream(), streamPart.size()));
		String eTag = response.eTag();
		CompletedPart part = CompletedPart.builder().partNumber(streamPart.getPartNumber()).eTag(eTag).build();
		
		partETags.add(part);

		log.info("{}: Finished uploading {}", this, streamPart);
	}

	@Override
	public String toString() {
		return String.format("[Manager uploading to %s/%s with id %s]", bucketName, putKey,
				Utils.skipMiddle(String.valueOf(uploadId), 21));
	}
}
