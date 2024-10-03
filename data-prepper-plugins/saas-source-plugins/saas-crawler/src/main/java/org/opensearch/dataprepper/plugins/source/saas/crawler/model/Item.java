/*
 *   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.opensearch.dataprepper.plugins.source.saas.crawler.model;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Optional;

/**
 * Base interface for modeling any item in a SAAS Service.
 */
public interface Item {

  /**
   * Returns the document ID of document.
   *
   * @return item id
   */
  String getDocumentId();

  /**
   * This method will be called by SDK to get item contents.
   *
   * <p>Implementer must also ensure that close() method of InputStream object they are returning
   * is idempotent. Idempotency is a requirement as {@link Closeable#close()} is supposed to be
   * idempotent and input stream implements closeable interface.
   *
   * @return {@link InputStream} object
   */
  InputStream getDocumentBody();

  /**
   * This method will be called by SDK to get document title.
   *
   * @return string representing document title
   */
  String getDocumentTitle();

  /**
   * This method will be called by SDK to understand document content type. If content type is not
   * known, then AWS Kendra will determine it automatically.
   *
   * @return {@link ContentType} object
   */
  ContentType getContentType();

  /**
   * This method is called by SDK to save additional crawl metadata so that partners can utilize
   * this information to send additional items in list items iterator, which they can't obtain from
   * repository anymore.
   *
   *
   * @return optional serialized crawl metadata.
   */
  default Optional<String> getCrawlMetadata() {
    return Optional.empty();
  }
}