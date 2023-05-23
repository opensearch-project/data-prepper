/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 *  Service that handle retry mechanism in case of any api failure
 */
public class BackoffService {
    private static final Logger LOG = LoggerFactory.getLogger(BackoffService.class);
 public int defaultRetries=2;

 public long defaultWaitTimeInMills=10000;

 private int numberOfRetries;
 
 private int numberOfTriesLeft;
 
 private long defaultTimeToWait = 1000;
 
 private long timeToWait;
 
 private final Random random = new Random();

 public BackoffService(int numberOfRetries){
     this.numberOfRetries = numberOfRetries;
     this.numberOfTriesLeft = numberOfRetries;
     this.timeToWait = defaultTimeToWait;
 }
 
 public boolean shouldRetry() {
     return numberOfTriesLeft > 0;
 }

 public void errorOccurred() {
   
   numberOfTriesLeft--;
   if (!shouldRetry()) {
      LOG.info("RETRY FAILED");
   }
   waitUntilNextTry();
   timeToWait += random.nextInt(1000);
 }

 public void waitUntilNextTry() {
  
  try {
    Thread.sleep(timeToWait);
  } catch (InterruptedException e) {
    LOG.error("InterruptedException ", e);
  }
 }
 public long getTimeToWait() {
   return this.timeToWait;
}

 public void doNotRetry() {
    numberOfTriesLeft = 0;
}

 public void reset() {
   this.numberOfTriesLeft = numberOfRetries;
   this.timeToWait = defaultTimeToWait;
 }

}