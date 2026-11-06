
* __Update fast-uri and brace-expansion to fix CVEs CVE-2026-6322,CVE-2026-6321 (#6969) (#6970)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Wed, 1 Jul 2026 07:33:19 -0700
    
    EAD -&gt; refs/heads/2.16, tag: refs/tags/2.16.0, refs/remotes/origin/2.16
    (cherry picked from commit 79b60ce92eb73fca8e567b415bbbac19c38212da)
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt; Signed-off-by: opensearch-ci-bot
    &lt;opensearch-infra@amazon.com&gt; Co-authored-by: Krishna Kondaka
    &lt;krishkdk@amazon.com&gt;

* __Prepare release 2.16.0 (#6948)__

    [opensearch-ci](mailto:83309141+opensearch-ci-bot@users.noreply.github.com) - Tue, 30 Jun 2026 15:16:32 -0700
    
    
    Signed-off-by: github-actions[bot]
    &lt;41898282+github-actions[bot]@users.noreply.github.com&gt; Co-authored-by:
    dlvenable &lt;293424+dlvenable@users.noreply.github.com&gt;

* __Fix file source infinite re-read in non-tail mode with codec (#6934)  (#6937)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Tue, 30 Jun 2026 08:54:16 -0700
    
    
    * Stop file source from rescheduling readers in non-tail mode (#6934)
    
    FileReaderPool.onReaderComplete inferred the &#34;should I reschedule?&#34; decision
    from the completed reader&#39;s RotationType, which has no terminal value. In
    non-tail mode any path that did not result in DELETED or CREATE_RENAME (notably
    NO_ROTATION and the codec one-shot path that never updates lastRotationType)
    was rescheduled every 500 ms, producing duplicate events.
    
    Make tail mode the single source of truth for rescheduling: when the reader
    completes in non-tail mode, mark the checkpoint completed, promote pending
    files, and exit. This restores the documented
    &#34;non-tail = read once, stop&#34; contract for the modern path and matches the
    behavior of the legacy ClassicFileStrategy.
    
    Resolves #6934
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Persist read offset after non-tail codec one-shot read (#6934)
    
    readFileWithCodecOneShot returned without updating the checkpoint entry, so a
    successful one-shot read advanced no offset. After the pool-side fix (no
    reschedule in non-tail mode), an in-process loop no longer occurs, but a
    restart would still re-read the file from offset 0 and produce duplicate
    events.
    
    After parseWithCodec returns true, advance readOffset, the checkpoint&#39;s read
    offset, and the committed offset to the file size. On parse failure the
    readErrors counter is incremented and the offsets stay at zero, matching the
    pre-fix semantics for the error path.
    
    Resolves #6934
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    ---------
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Update dependency aws-cdk-lib to v2.254.0 (#6920)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Tue, 30 Jun 2026 08:51:48 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Updates the release process to run automatically and fully with new OpenSearch project rules. (#6921)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Jun 2026 08:15:18 -0700
    
    
    Updates the release process to run automatically and fully with new OpenSearch
    project rules.
    
    The OpenSearch project no longer supports tagging in workflows or use of app
    tokens. The release GitHub Action currently creates the tag for a release. Now
    that this is unsupported, the release GHA validates that the tag exists and
    matches what we expect from the gradle.properties file. Both the release and
    release-prepare-branch GitHub Actions currently create PRs against the repo.
    The new approach uses a dedicated fork for creating these PRs. Access to the
    fork is granted via a personal access token. This commit also updates related
    documentation for setting up a repository for releases.
    
    Resolves #6912.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Disable FAIL_ON_UNKNOWN_PROPERTIES in SqsMessageParser ObjectMapper (#6935)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Fri, 19 Jun 2026 12:19:23 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Add multiline input codec for grouping multi-line log events (#6911)__

    [yavmanis](mailto:yavmanis@amazon.com) - Thu, 18 Jun 2026 12:10:25 -0500
    
    
    Signed-off-by: Manisha Yadav &lt;yavmanis@amazon.com&gt;

* __Honor fractional Retry-After header values (#6928)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Thu, 18 Jun 2026 11:19:14 -0500
    
    
    RetryAfterHeaderStrategy parsed the Retry-After header with Integer.parseInt,
    which throws on fractional second values that some HTTP services return (e.g.
    299.997). The exception was swallowed and the header ignored, so the client
    fell back to a short fixed backoff and retried far sooner than the server
    requested. Under HTTP 429 this collapses one backoff window into a burst of
    premature retries.
    
    Parse the value as a decimal and round up so the client never waits less than
    requested. Guard against non-finite and excessively large values, which the
    decimal parser would otherwise accept where the integer parser previously
    rejected them, to avoid integer overflow in the sleep calculation.
    
    Resolves #6927
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Handling unknown fields in s3 event notifications (#6933)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Wed, 17 Jun 2026 14:14:33 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Fix bug in s3 eventbridge notification where a new field was introduced (#6932)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Wed, 17 Jun 2026 11:28:53 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Update the Claude command to generate release notes with some allowed tools that it needs. (#6918)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 16 Jun 2026 08:52:23 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __(Temporary) Add region override for aws config (#6926)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Sun, 14 Jun 2026 21:54:46 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Set &#34;drop&#34; as an alias for noop sink (#6922)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Fri, 12 Jun 2026 12:27:56 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Upgrade aws-cdk-lib to 2.253.1 for security vulnerabilities (#6913)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Thu, 11 Jun 2026 06:34:48 -0700
    
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Optimize MongoDBExportPartitionSupplier for uniform _id type collections (#6910)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 9 Jun 2026 11:47:07 -0500
    
    
    For collections with uniform _id types, replace the 8-clause $or query with a
    simple Filters.gt(&#34;_id&#34;, value) for finding partition boundaries. This allows
    DocumentDB to use a single B-tree index seek instead of multi-index scan.
    
    Changes:
    - Add isUniformIdType() that checks first/last doc _id types
    - Add buildNextStartFilter() with simple $gt for uniform types,
     falling back to $or-based query for mixed types
    - Use fresh Filters.gte() + skip() per iteration for partition end
    - Extract addPartition() helper to reduce duplication
    - Make BsonHelper.isClassNumber() public for numeric type grouping
    
    Performance: 14M docs (10GB) partitioned in ~30 seconds.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Update deprecated GitHub Actions to versions already used in the project (#6906)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Jun 2026 14:23:43 -0700
    
    
    Bumps deprecated action references in workflow files to the highest SHA already
    pinned elsewhere in this repository:
    
    - actions/checkout v2/v4 -&gt; v6 (de0fac2e)
    - actions/setup-node v2 -&gt; v4 (49933ea5)
    - actions/github-script v6 -&gt; v8 (ed597411)
    - peter-evans/create-pull-request v4 -&gt; v6 (c5a78066)
    
    Two deprecated actions remain that have no newer version in this project and
    are not changed here: codecov/codecov-action@v1 in gradle.yml and
    EnricoMi/publish-unit-test-result-action@v1 in six workflows.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.mock-server:mockserver-junit-jupiter-no-dependencies (#6897)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 14:19:10 -0700
    
    
    Bumps org.mock-server:mockserver-junit-jupiter-no-dependencies from 5.15.0 to
    6.1.0.
    
    --- updated-dependencies:
    - dependency-name: org.mock-server:mockserver-junit-jupiter-no-dependencies
     dependency-version: 6.1.0
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __add logging to track s3 records processed per object and show which object was completed when messages are deleted (#6908)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Fri, 5 Jun 2026 13:33:36 -0700
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Bump ajv from 6.12.6 to 6.14.0 in /testing/aws-testing-cdk (#6557)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:33:02 -0700
    
    
    Bumps [ajv](https://github.com/ajv-validator/ajv) from 6.12.6 to 6.14.0.
    - [Release notes](https://github.com/ajv-validator/ajv/releases)
    - [Commits](https://github.com/ajv-validator/ajv/compare/v6.12.6...v6.14.0)
    
    --- updated-dependencies:
    - dependency-name: ajv
     dependency-version: 6.14.0
     dependency-type: indirect
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.github.seancfoley:ipaddress in /data-prepper-expression (#6697)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:31:37 -0700
    
    
    Bumps
    [com.github.seancfoley:ipaddress](https://github.com/seancfoley/IPAddress) from
    5.5.1 to 5.6.2.
    - [Release notes](https://github.com/seancfoley/IPAddress/releases)
    - [Commits](https://github.com/seancfoley/IPAddress/compare/v5.5.1...v5.6.2)
    
    --- updated-dependencies:
    - dependency-name: com.github.seancfoley:ipaddress
     dependency-version: 5.6.2
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.json:json in /data-prepper-plugins/avro-codecs (#6898)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:29:49 -0700
    
    
    Bumps [org.json:json](https://github.com/douglascrockford/JSON-java) from
    20251224 to 20260522.
    - [Release notes](https://github.com/douglascrockford/JSON-java/releases)
    -
    [Changelog](https://github.com/stleary/JSON-java/blob/master/docs/RELEASES.md)
    -
    [Commits](https://github.com/douglascrockford/JSON-java/compare/20251224...20260522)
    
    --- updated-dependencies:
    - dependency-name: org.json:json
     dependency-version: &#39;20260522&#39;
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#6899)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:29:36 -0700
    
    
    Bumps software.amazon.awssdk:auth from 2.43.2 to 2.45.1.
    
    --- updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-version: 2.45.1
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact in /data-prepper-plugins/opensearch (#6893)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:26:47 -0700
    
    
    Bumps org.apache.maven:maven-artifact from 3.9.15 to 3.9.16.
    
    --- updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-version: 3.9.16
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-core (#6894)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:26:38 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.25.3 to 2.26.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.25.3...rel/2.26.0)
    
    --- updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-version: 2.26.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.github.luben:zstd-jni in /data-prepper-plugins/common (#6895)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:26:33 -0700
    
    
    Bumps [com.github.luben:zstd-jni](https://github.com/luben/zstd-jni) from
    1.5.7-8 to 1.5.7-9.
    - [Commits](https://github.com/luben/zstd-jni/compare/v1.5.7-8...v1.5.7-9)
    
    --- updated-dependencies:
    - dependency-name: com.github.luben:zstd-jni
     dependency-version: 1.5.7-9
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.logging.log4j:log4j-bom in /data-prepper-expression (#6896)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 5 Jun 2026 13:25:27 -0700
    
    
    Bumps
    [org.apache.logging.log4j:log4j-bom](https://github.com/apache/logging-log4j2)
    from 2.25.3 to 2.26.0.
    - [Release notes](https://github.com/apache/logging-log4j2/releases)
    -
    [Changelog](https://github.com/apache/logging-log4j2/blob/2.x/RELEASE-NOTES.adoc)
    -
    [Commits](https://github.com/apache/logging-log4j2/compare/rel/2.25.3...rel/2.26.0)
    
    --- updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-version: 2.26.0
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Add tail mode to file source for continuous log tailing  (#6853)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Fri, 5 Jun 2026 15:03:29 -0500
    
    
    Add tail mode to file source for continuous log tailing
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Adds project rules and steering for Claude Code and Kiro. (#6907)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 5 Jun 2026 09:07:00 -0700
    
    
    We already have some developer documentation that is relevant for AI tools as
    well as people. This adds instructions for Claude and Kiro to use them. For
    Claude Code it uses includes and for Kiro it is a symlink in the steering
    directory.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add legacy MD5 checksum validation (#6790)__

    [Simon Elbaz](mailto:elbazsimon9@gmail.com) - Fri, 5 Jun 2026 07:53:00 -0700
    
    
    S3 DLQ: Add path style (deprecated) access and legacy MD5 checksum validation
    
    Signed-off-by: Simon ELBAZ &lt;elbazsimon9@gmail.com&gt;

* __Add support for named configurations in aws config block (#6905)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 4 Jun 2026 10:19:51 -0700
    
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add configurable path_prefix parameter (#6674)__

    [Luis Pigueiras](mailto:thenewpigue819@gmail.com) - Thu, 4 Jun 2026 06:41:07 -0700
    
    
    Introduce an optional `path_prefix` sink parameter to support OpenSearch 
    instances served behind a reverse proxy under a custom subdirectory.
    
    Resolves #6654
    
    Signed-off-by: Luis Pigueiras &lt;luis.pigueiras@cern.ch&gt;

* __Fix initial load completion tracking race condition (#6711)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Thu, 4 Jun 2026 06:20:35 -0700
    
    
    Move the completion GlobalState creation before the task partition creation
    loop in performInitialLoad(). Previously, the completion key was created after
    all partitions, allowing workers to finish and call 
    incrementSnapshotCompletionCount() before the key existed. Those increments
    were silently lost, causing waitForSnapshotComplete() to never reach the
    expected total.
    
    Verify that GlobalState (completion tracking) is created before 
    InitialLoadTaskPartition, ensuring workers can report completion as soon as
    they acquire a partition.
    
    Fix LeaderSchedulerTest to pass shuffle parameters to updated constructor
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Removes unused S3ClientFactory class from s3_enrich processor. (#6904)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 3 Jun 2026 15:38:43 -0700
    
    
    This class is not used and is not setting up the client in the standard way
    anyway.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support PluginConfigVariable for Confluence/Jira bearer token.          (#6856)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Wed, 3 Jun 2026 16:37:23 -0500
    
    
    * Change bearer_token from String to PluginConfigVariable for secrets manager
    integration
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Use instanceof for safe cast, guard refresh with isUpdatable, consolidate
    token validation into helper method
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Remove unused UUID import from AtlassianAuthFactoryTest
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Remove isUpdatable guard and call refresh directly per reviewer feedback
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    ---------
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Replace unsafe String casts with instanceof check in AtlassianOauthConfig (#6875)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Wed, 3 Jun 2026 16:33:31 -0500
    
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Update dependency aws-cdk-lib to v2.248.0 (#6884)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Wed, 3 Jun 2026 12:06:31 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Add Throwable safety net to CloudWatch Logs sink Uploader (#6888)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 3 Jun 2026 12:05:44 -0700
    
    
    When an unchecked Throwable escaped Uploader.upload(), the executor worker
    thread terminated silently. The batch&#39;s events were never acknowledged, never
    DLQ&#39;d, and never reflected in any metric — a fatal classpath or runtime issue
    produced no operator-visible signal beyond a single stderr line from
    ThreadPoolExecutor. This change makes such failures observable and recoverable:
    the unhandled-error path now emits a dedicated metric and structured log,
    accounts the lost events in the existing failure counter, and releases event
    handles so the source can make forward progress instead of waiting indefinitely
    for an ack.
    
    Closing the safety gap also required fixing a latent correctness bug that a
    naive catch-all would have compounded. Success and failure metrics were
    incremented before the post-loop cleanup ran, so a Throwable escaping after
    that point would have produced succeeded + failed &gt; total — breaking any
    percentage-based dashboard or
    &#34;no events failed&#34; alarm. Without addressing this, the safety net would have
    traded silent loss for inconsistent metrics.
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Fix bug that would cause ddb stream processing to start before export completed (#6892)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Sat, 30 May 2026 14:56:31 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix OpenSearch source pagination to handle failures correctly (#6891)__

    [Keyur Patel](mailto:keyurpatel.opensource@gmail.com) - Sat, 30 May 2026 11:02:41 -0500
    
    
    Pagination previously terminated whenever a page returned fewer documents than
    the configured batch_size, which silently dropped the rest of an index whenever
    a request hit partial shard failures or a transient error. The correct
    termination signal is used instead: nextSearchAfter == null / empty page for
    search_after and PIT workers, and an empty page for the scroll worker.
    
    Shard failures are now captured in a bounded map of normalized reason
    -&gt; count (capped at 20 distinct keys with an &#34;__other__&#34; overflow bucket),
    persisted on OpenSearchIndexProgressState, surfaced as new counters
    (searchShardsFailed, searchRequestsFailed, indicesCompletedWithFailures), and
    logged per page plus once at index completion.
    
    The scroll worker no longer aborts an index on a single per-request exception;
    it tolerates up to MAX_CONSECUTIVE_SCROLL_FAILURES retries before giving up the
    partition.
    
    Signed-off-by: Keyur-S-Patel &lt;keyurpatel.opensource@gmail.com&gt;

* __Log uncaught errors in S3 scan worker to prevent silent thread death (#6890)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Fri, 29 May 2026 15:41:37 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add TransformFunctionProvider to DynamicConfigTransformer (#6881)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 28 May 2026 15:42:08 -0700
    
    
    * Add TransformFunctionProvider to DynamicConfigTransformer
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed checkstyle errors
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed License headers
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing coverage tests in aws-plugin
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed integration test failures
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Initial addition of the PullIngester for the opensearch sink. (#6842)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 May 2026 13:34:37 -0700
    
    
    Initial addition of the PullIngester for the opensearch sink.
    
    This creates a new PullIngester and implements the first one as the
    KafkaPullEngine. It reads an index to find the pull ingestion topic and then
    writes data to that topic. It routes shards using the same Murmur 3 approach
    that OpenSearch uses. The pull-based ingestion is marked as experimental. The
    configuration requires specifying the document Id currently.
    
    Resolves #6835
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support automatic plugin loading in Data Prepper core. (#6882)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 28 May 2026 13:33:07 -0700
    
    
    This change allows plugin authors to load other plugins by defining
    @UsesDataPrepperPlugin on a field with a different type. It will automatically
    load the plugin using the PluginFactory and give the main plugin the desired
    plugin instance that they are looking for.
    
    This updates the sqs source to use this new capability for its InputCodec
    defined by the codec property.
    
    Resolves #4838
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Attempt to fix the integration test for Kinesis source (#6886)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Thu, 28 May 2026 05:34:25 -0700
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt; Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Adding support for rate_limiter processor (#6872)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Wed, 27 May 2026 12:16:18 -0700
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Add Entity config support to sink (#6864)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Wed, 27 May 2026 11:22:35 -0700
    
    
    Add Entity config support to sink
    
    Add an optional &#39;entity&#39; configuration block on the CloudWatch Logs sink that
    attaches CloudWatch Entity metadata (key_attributes + attributes) to every
    PutLogEvents request, enabling entity-based correlation in CloudWatch.
    
    When &#39;entity&#39; is omitted the sink behaves identically to before. When 
    configured, an SDK Entity is built and passed to the dispatcher via the 
    builder; the dispatcher conditionally sets it on each PLE request.
    
    If CloudWatch rejects the entity, the request is still considered successful
    (events are released), a WARN log records the RejectedEntityInfo error type,
    and a new &#39;cloudWatchLogsEntityRejected&#39; counter is incremented as the primary
    alarmable signal.
    
    Validation is intentionally minimal — @NotEmpty on key_attributes only. 
    AWS-owned limits (max entries, allowed key names, length caps) are enforced
    server-side and surfaced via the rejection metric, avoiding client-side drift
    when the Entity API contract changes.
    
    Tests follow TDD: each test was written before the code that makes it pass. New
    unit tests cover EntityConfig defaults, deserialization, and
    @NotEmpty validation; @Valid cascade from CloudWatchLogsSinkConfig; the new
    counter; dispatcher entity-on-request, no-entity, and rejection paths; and sink
    wiring of EntityConfig -&gt; Entity -&gt; dispatcher. TestSinkOperationWithEntity
    integration test exercises the SDK end-to-end.
    
    Acceptance criteria from the spec are met: backward compatibility, entity
    attached when configured, non-fatal rejection with metric and log, all existing
    tests green, new tests cover new code paths, license headers on new files.
    
    Add full license headers to EntityConfig and EntityConfigTest
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Fix the integ test for kds workflow (#6885)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Wed, 27 May 2026 10:34:41 -0700
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt; Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Update the version of KCL from 2.6.0 to 2.7.2 (#6883)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Wed, 27 May 2026 09:41:27 -0700
    
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt; Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __Pin GitHub Actions to commit SHAs for supply chain security (#6880)__

    [Divya Madala](mailto:113469545+Divyaasm@users.noreply.github.com) - Tue, 26 May 2026 11:59:33 -0700
    
    
    Signed-off-by: Divya Madala &lt;divyaasm@amazon.com&gt;

* __Make ImmutablePluginConfigVariable.refresh() a no-op instead of throwing. (#6869)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 22 May 2026 09:52:14 -0700
    
    
    The refresh() method semantically means &#34;re-read from your backing store.&#34; For
    an immutable variable with no backing store, this is naturally a no-op. Only
    setValue() should throw since it attempts to mutate the value. This allows
    callers to safely call refresh() without needing to guard with isUpdatable()
    checks.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix KafkaSourceJsonTypeIT: update kafka_headers type from byte[] to String (#6866)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Fri, 22 May 2026 09:03:46 -0700
    
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Add issues write permission to untriaged label workflow (#6873)__

    [Shreya Bhatta](mailto:shreyab963@gmail.com) - Thu, 21 May 2026 15:02:19 -0700
    
    
    Signed-off-by: shreyah963 &lt;shreyab963@gmail.com&gt;

* __Bump urllib3 in /examples/trace-analytics-sample-app/sample-app (#6851)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 21 May 2026 08:44:33 -0700
    
    
    Bumps [urllib3](https://github.com/urllib3/urllib3) from 2.6.3 to 2.7.0.
    - [Release notes](https://github.com/urllib3/urllib3/releases)
    - [Changelog](https://github.com/urllib3/urllib3/blob/main/CHANGES.rst)
    - [Commits](https://github.com/urllib3/urllib3/compare/2.6.3...2.7.0)
    
    --- updated-dependencies:
    - dependency-name: urllib3
     dependency-version: 2.7.0
     dependency-type: direct:production
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Fix typo: occured -&gt; occurred in ml-inference processors (#6847)__

    [Sai Asish Y](mailto:say.apm35@gmail.com) - Thu, 21 May 2026 08:07:54 -0700
    
    
    Signed-off-by: SAY-5 &lt;saiasish.cnp@gmail.com&gt; Co-authored-by: SAY-5
    &lt;saiasish.cnp@gmail.com&gt;

* __Fix typo: occured -&gt; occurred (#6846)__

    [Sai Asish Y](mailto:say.apm35@gmail.com) - Thu, 21 May 2026 08:07:36 -0700
    
    
    Signed-off-by: SAY-5 &lt;saiasish.cnp@gmail.com&gt; Co-authored-by: SAY-5
    &lt;saiasish.cnp@gmail.com&gt;

* __Fix typo: occured -&gt; occurred (#6845)__

    [Sai Asish Y](mailto:say.apm35@gmail.com) - Thu, 21 May 2026 08:07:31 -0700
    
    
    Signed-off-by: SAY-5 &lt;saiasish.cnp@gmail.com&gt; Co-authored-by: SAY-5
    &lt;saiasish.cnp@gmail.com&gt;

* __Upgrade jenkins library version to accomodate new secrets vault (#6871)__

    [Sayali Gaikawad](mailto:gaiksaya@amazon.com) - Thu, 21 May 2026 07:03:17 -0700
    
    
    Signed-off-by: Sayali Gaikawad &lt;gaiksaya@amazon.com&gt;

* __Create log group and stream in CloudWatch Logs sink (#6863)__

    [Nikhil Bagmar](mailto:40037072+bagmarnikhil@users.noreply.github.com) - Thu, 21 May 2026 06:20:13 -0700
    
    
    Create log group and stream in CloudWatch Logs sink
    
    When enabled, the CloudWatch Logs sink creates the configured log group and log
    stream on first ResourceNotFoundException instead of failing to DLQ.
    
    This eliminates the need for manual pre-provisioning of CloudWatch Logs
    resources before running Data Prepper pipelines.
    
    - Reactive creation (on-failure), not eager (at-init)
    - Creation attempted at most once per Uploader invocation
    - Idempotent: ResourceAlreadyExistsException silently ignored
    - Requires logs:CreateLogGroup and logs:CreateLogStream IAM perms
    
    Split resource creation into separate create_log_group and create_log_stream
    config options
    
    - create_log_group (default false): creates log group on
    ResourceNotFoundException
    - create_log_stream (default true): creates log stream on
    ResourceNotFoundException
    - Dispatcher only calls createLogGroup/createLogStream based on respective
    flags
    - Expand acronyms in comments and test method names for readability
    - Add tests for independent flag combinations (only group, only stream)
    
    Change create_log_stream default to false for backward compatibility
    
    The create_log_stream config option previously defaulted to true, which changes
    existing sink behavior by attempting stream creation on 
    ResourceNotFoundException. Default both create_log_group and create_log_stream
    to false so existing pipelines are unaffected and users must explicitly opt in
    to auto-creation.
    
    Resolves: #6861
    
    ---------
    
    Signed-off-by: Nikhil Bagmar &lt;nikhilbagmar73@gmail.com&gt;

* __Adds Srikanth Padakanti (srikanthpadakanti) as a maintainer. (#6870)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 20 May 2026 13:06:40 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix and extend AWS environment derivation for OTLP resource attributes (#6857)__

    [Shenoy Pratik](mailto:pshenoy36@gmail.com) - Wed, 20 May 2026 09:32:53 -0700
    
    
    * Fix and extend AWS environment derivation for OTLP resource attributes
    
    Signed-off-by: ps48 &lt;pshenoy36@gmail.com&gt;
    
    * added @Nested, remove unused imports
    
    Signed-off-by: ps48 &lt;pshenoy36@gmail.com&gt;
    
    ---------
    
    Signed-off-by: ps48 &lt;pshenoy36@gmail.com&gt;

* __Use the setup-gradle action to improve build times through caching. (#6868)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 19 May 2026 13:39:19 -0700
    
    
    Use the setup-gradle action (at 6.1.0) to add caching to our builds. Update all
    setup-java actions to 5.2.0. Use commit SHAs for actions rather than version
    numbers for additional security on version changes.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix s3 folder scan depth for RDS source when partition prefix is not set (#6859)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Tue, 19 May 2026 10:41:14 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Support for otel_traces codec to create Span Events from OTEL Traces (#6843)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Tue, 19 May 2026 10:40:57 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Add test result artifact upload to raw span peer forwarder e2e workflow (#6867)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 19 May 2026 08:17:00 -0700
    
    
    Ref #6721
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Moves the creation of OpenSearch clients back into the initalize() method. (#6855)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 15 May 2026 14:25:30 -0700
    
    
    Data Prepper doesn&#39;t have strong lifecycles beyond initialize() and shutdown().
    So creating multiple pipelines or attempting to initialize over time resulted
    in running out of files. This reverts the change to use the constructor until a
    better way to close plugins is available.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add Confluence Data Center support with allow_internal_address and beare… (#6769)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 11 May 2026 09:30:06 -0700
    
    
    * Add Confluence Data Center support with allow_internal_address and bearer
    token auth
    
    Make address validation configurable via allow_internal_address (default false) 
    so Confluence Data Center on internal networks is supported. Add bearer token 
    authentication for Personal Access Tokens used by Data Center deployments.
    
    Resolves #6496
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add lookup_private_addresses option to GeoIP processor for internal I… (#6770)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 11 May 2026 08:19:08 -0700
    
    
    Add lookup_private_addresses option to GeoIP processor for internal IP
    enrichment
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Extract shared HTTP client auth into http-client-common module (#6776)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 11 May 2026 08:18:34 -0700
    
    
    Extract shared HTTP client auth into http-client-common module. Add null/blank
    validation to auth constructors and config.
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Extract shared Prometheus metric utilities for output consistency  (#6830)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 11 May 2026 08:13:59 -0700
    
    
    * Extract shared metric utilities into PrometheusMetricUtils with cross-parser
    consistency test
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    * Remove wrapper methods, call PrometheusMetricUtils directly per review
    feedback
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;
    
    ---------
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add array field splitting support to split_event processor (#6774)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Mon, 11 May 2026 08:06:57 -0700
    
    
    Add array field splitting support to split_event processor
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Improves error handling for peer-forwarder. (#6833)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 May 2026 10:03:34 -0700
    
    
    Test scenarios where the input is incorrect and verify the correct response
    codes. Fix a 500 error when the target pipeline and plugin do not exist by
    returning 400 instead.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Release notes for Data Prepper 2.15.1 (#6831)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 May 2026 08:27:16 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes the opensearch sink build. (#6832)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 7 May 2026 07:53:14 -0700
    
    
    The ConnectionConfiguration is missing probably from some of the merges along
    with recent refactoring. The fix is to create a field for it in the constructor
    and use it when initializing.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Auto generate test PEM files at runtime using BouncyCastle (#6827)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Thu, 7 May 2026 06:32:26 -0700
    
    
    Auto generate test PEM files at runtime using BouncyCastle
    
    Use @TempDir for PEM file cleanup instead of deleteOnExit
    
    The connectionConfiguration local variable was moved out of 
    doInitializeInternal scope by PR #6795. Use the getter from 
    openSearchSinkConfig instead.
    
    Fix mTLS integration test to present client cert on verification client
    
    The verification RestClient also needs to present a client certificate when
    OpenSearch has clientauth_mode: REQUIRE. Added a local client builder with
    client cert support. Also simplified the workflow mTLS config logic with prefix
    detection.
    
     Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add pipeline DLQ support for rds source (#6817)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Wed, 6 May 2026 23:47:13 -0500
    
    
    * Add pipeline DLQ support for rds source stream path
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add unit tests for sendToFailurePipeline in stream path
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Add pipeline DLQ support for rds source export path
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Preserve original throw behavior in export path when DLQ is not configured
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Generate JSON schemas for plugins with schema definitions for nested configurations (#6814)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 May 2026 13:23:51 -0700
    
    
    Support JSON schema generation with schema definitions for nested types instead
    of using object.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add semantic enrichment support for OpenSearch sink index creation (#6771)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Wed, 6 May 2026 12:35:58 -0700
    
    
    * Add semantic enrichment support for OpenSearch sink index creation
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed license headers
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Modified to use AWS SDK instead of http endpoint
    for creating semantic enrichment enabled indexes
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing end2end tests by creating a SemanticFieldMapping as a separate
    class
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing license checks. Addressed comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed failing license checks. Addressed comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Initial support for experimental features within Data Prepper plugins. (#6811)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 May 2026 11:38:44 -0700
    
    
    Initial support for experimental features within Data Prepper plugins.
    
    This allows plugin authors to add an @Experimental annotation to configuration
    fields where the default value is null. This works well especially for nested
    configurations.
    
    The approach to implementing this is to expand our Hibernate Validator to be
    able to get custom ConstraintValidator instances from the Spring application
    context. This adds an ExperimentalFeatureValidator which implements
    ConstraintValidator for the @Experimental annotation. This allows
    data-prepper-api to retain the @Experimental annotation but not have any
    knowledge of how the constraint is implemented.
    
    Exclude the new annotation elements added to @Experimental from the
    compatibility test. These elements are not breaking backward compatibility.
    They have default values and Java handles them at runtime. However, the byte
    code does generate abstract methods, which are generally backward incompatible.
    That is why it is flagged as a breaking change.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactors the OpenSearch sink to split out the ingestion work from other work such as index management. I plan to use this work to help support pull-based ingestion within this sink. This commit is just for refactoring since it is already significant. (#6795)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 6 May 2026 11:38:03 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add internal connectors to external ML providers (Bedrock/SageMaker, etc) in the ml_inference processor (#6772)__

    [Xun Zhang](mailto:xunzh@amazon.com) - Wed, 6 May 2026 09:57:19 -0700
    
    
    * Add internal connectors to bedrock/sagemaker
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * add more logs and fix the checkstyleTest
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * refactor to use batch predictor interphase in BedrockBatchJobCreator and add
    nova embedding model connector
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    * use reflections to list models and remove redundant tests
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Xun Zhang &lt;xunzh@amazon.com&gt;

* __Allow Parenthsis (#6233)__

    [Utkarsh Agarwal](mailto:126544832+Utkarsh-Aga@users.noreply.github.com) - Wed, 6 May 2026 08:16:41 -0700
    
    
    Signed-off-by: Utkarsh Agarwal &lt;126544832+Utkarsh-Aga@users.noreply.github.com&gt;

* __Fix delete_source removing parsed field when writing to root in parse JSON processor (#6443)__

    [DayneD89](mailto:dayned89@gmail.com) - Wed, 6 May 2026 08:15:03 -0700
    
    
    When source field name matches a key in the parsed JSON and delete_source is
    true with destination set to root (the default), the source was deleted after
    writeToRoot(), removing the parsed value instead of the original. Now deletes
    source before writeToRoot() when writing to root.
    
    Signed-off-by: DayneD89 &lt;DayneD89@gmail.com&gt;

* __Renew partition lease in DataFileLoader to prevent reprocessing (#6821)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Tue, 5 May 2026 19:26:56 -0500
    
    
    * Renew partition lease in DataFileLoader to prevent reprocessing
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    * Throw exception on flush failure so scheduler can give up partition
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Add client certificate authentication (mTLS) to OpenSearch sink (#6755)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Tue, 5 May 2026 15:10:54 -0700
    
    
    Add client certificate authentication (mTLS) to OpenSearch sink
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add source-layer shuffle to iceberg-source for correct and scalable C… (#6682)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 5 May 2026 09:25:00 -0700
    
    
    * Add source-layer shuffle to iceberg-source for correct and scalable CDC
    processing
    
    Introduce a pull-based shuffle mechanism for processing snapshots that contain 
    DELETE operations (UPDATE/DELETE in Copy-on-Write tables). When a snapshot
    contains DeletedDataFileScanTasks, records are shuffled by identifier_columns
    hash across nodes so that carryover removal and UPDATE merge operate on
    complete data, including cross-partition updates.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * skip updating lastsnapshotId when shuffle failed
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * spotless apply
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add S3 certificate path support and ssl_insecure_disable_verification to
    shuffle server
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Fix coalesce to collect index from all nodes and extract ShuffleNodeClient
    utility
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add remote shuffle file cleanup via HTTP DELETE endpoint on all nodes
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Fix shuffle write completion key race condition by creating GlobalState
    before partitions
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Fix shuffle Avro serialization to use Iceberg DataWriter/PlannedDataReader
    for correct type handling
    
    The shuffle record serialization used GenericDatumWriter/GenericDatumReader
    which only handle Avro native types. Iceberg Records contain Java types like
    OffsetDateTime for timestamptz columns, causing AvroRuntimeException during
    SHUFFLE_WRITE.
    
    Replace with Iceberg&#39;s DataWriter and PlannedDataReader which handle the
    Iceberg-to-Avro type conversion internally. Extract serialization logic into
    RecordAvroSerializer utility class with roundtrip tests covering temporal
    types.
    
    Also fix shuffle write completion key race condition by creating GlobalState
    before partitions in processShuffleSnapshot, matching the order used in
    processInsertOnlySnapshot.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Use common HTTP server infrastructure for shuffle
    
    Migrate ShuffleHttpServer to use CreateServer from http-common and 
    ShuffleNodeClient to use Armeria WebClient. ShuffleConfig now extends 
    BaseHttpServerConfig for consistent TLS configuration including ACM and S3
    certificate support.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Replace fully qualified ByteBuffer with import in ShuffleHttpService
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Replace magic numbers with named constants in shuffle writer and reader
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add path traversal protection to LocalDiskShuffleStorage
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add input validation to shuffle HTTP endpoints with tests
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Improve error handling in shuffle HTTP endpoints
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Support authentication plugin for shuffle HTTP server
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Make shuffle storage path configurable and fix flaky test
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add retry limit to DynamoDB write loops in ChangelogWorker
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Use toString-based hash for deterministic shuffle partitioning across JVMs
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Isolate shuffle storage per node and preserve base directory on cleanup
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * fix import
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Add mutual TLS support for shuffle node-to-node communication
    
    Add mTLS authentication for shuffle HTTP server and client, following the same
    approach as PeerForwarder. All nodes share the same certificate and use it for
    both server TLS and client authentication.
    
    Changes:
    - Add mutualTls parameter to CreateServer lightweight HTTP server
    - Add ssl_client_auth config option to ShuffleConfig
    - Configure ShuffleNodeClient with keyManager and trustManager for mTLS
    - Remove plugin-based authentication (ArmeriaHttpAuthenticationProvider)
     in favor of TLS-layer authentication
    - Remove unused PluginFactory from IcebergSource and IcebergService
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Fix compilation failure in IcebergSourceIT
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    * Address review feedback on shuffle implementation
    
    - Use try-with-resources for ShuffleReader in ShuffleHttpService
    - Unwrap CompletionException in ShuffleNodeClient retry logging
    - Remove unnecessary try-with-resources on AggregatedHttpResponse.content()
    - Add debug log for shuffle write location type in LeaderScheduler
    - Add comment explaining Math.floorMod usage in computeShufflePartition
    - Move lz4-java dependency to version catalog
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;
    
    ---------
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Fix flaky DefaultAcknowledgementSetManagerTests.testExpirations (#6720)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 5 May 2026 09:22:46 -0700
    
    
    Move the monitor size assertion into the await().untilAsserted() block. The
    assertion was placed after Thread.sleep() but before await(), assuming the
    cleanup thread had already run. Under CI load, the cleanup may not have
    completed in time, causing intermittent failures.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Add index_type tsdb support to OpenSearch sink for Prometheus metrics (#6691)__

    [Srikanth Padakanti](mailto:srikanth_padakanti@apple.com) - Tue, 5 May 2026 07:46:42 -0700
    
    
    Add index_type tsdb support to OpenSearch sink for Prometheus metrics
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Fix IndexOutOfBoundsException in binlog event processing when row data and column names have different lengths (#6815)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 4 May 2026 14:14:03 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Handle MySQL ENUM value 0 in StringTypeHandler (#6816)__

    [Hai Yan](mailto:8153134+oeyh@users.noreply.github.com) - Mon, 4 May 2026 14:13:53 -0500
    
    
    Signed-off-by: Hai Yan &lt;oeyh@amazon.com&gt;

* __Build the whole data-prepper-test project as part of releases. (#6818)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 4 May 2026 11:10:11 -0700
    
    
    When the original projects were moved into data-prepper-test (commit 65fab21f)
    the whole sub-project should have been included.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes the Docker release build on local machines that may have multiple versions. (#6813)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 4 May 2026 10:37:30 -0700
    
    
    If you build the Docker task between versions, some files would remain that
    would cause the Docker build to fail. Delete the build Docker directory before
    running Docker build to clean out any old versions.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fixes the opensearch sink tests that run against OpenDistro. These were broken by a611740 from PR #6647 which only disabled them for ES 6. But there are some versions of OpenDistro based on ES 7 that don&#39;t support composable index templates. They were added in OpenDistro 1.9. (#6812)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 1 May 2026 10:02:13 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump org.projectlombok:lombok in /data-prepper-plugins/opensearch (#6805)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 09:01:38 -0700
    
    
    Bumps [org.projectlombok:lombok](https://github.com/projectlombok/lombok) from
    1.18.42 to 1.18.46.
    -
    [Changelog](https://github.com/projectlombok/lombok/blob/master/doc/changelog.markdown)
    -
    [Commits](https://github.com/projectlombok/lombok/compare/v1.18.42...v1.18.46)
    
    --- updated-dependencies:
    - dependency-name: org.projectlombok:lombok
     dependency-version: 1.18.46
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.httpcomponents.client5:httpclient5 (#6803)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 08:25:48 -0700
    
    
    Bumps
    [org.apache.httpcomponents.client5:httpclient5](https://github.com/apache/httpcomponents-client)
    from 5.6 to 5.6.1.
    -
    [Changelog](https://github.com/apache/httpcomponents-client/blob/rel/v5.6.1/RELEASE_NOTES.txt)
    -
    [Commits](https://github.com/apache/httpcomponents-client/compare/rel/v5.6...rel/v5.6.1)
    
    --- updated-dependencies:
    - dependency-name: org.apache.httpcomponents.client5:httpclient5
     dependency-version: 5.6.1
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump software.amazon.awssdk:auth in /performance-test (#6801)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 08:25:22 -0700
    
    
    Bumps software.amazon.awssdk:auth from 2.41.19 to 2.43.2.
    
    --- updated-dependencies:
    - dependency-name: software.amazon.awssdk:auth
     dependency-version: 2.43.2
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump org.apache.maven:maven-artifact in /data-prepper-plugins/opensearch (#6799)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 08:25:01 -0700
    
    
    Bumps org.apache.maven:maven-artifact from 3.9.12 to 3.9.15.
    
    --- updated-dependencies:
    - dependency-name: org.apache.maven:maven-artifact
     dependency-version: 3.9.15
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump com.github.luben:zstd-jni in /data-prepper-plugins/common (#6798)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 08:24:29 -0700
    
    
    Bumps [com.github.luben:zstd-jni](https://github.com/luben/zstd-jni) from
    1.5.7-6 to 1.5.7-8.
    - [Commits](https://github.com/luben/zstd-jni/commits)
    
    --- updated-dependencies:
    - dependency-name: com.github.luben:zstd-jni
     dependency-version: 1.5.7-8
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/rss-source (#6800)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 05:49:46 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.14.0
    to 2.14.2.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.14.0...v2.14.2)
    
    --- updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-version: 2.14.2
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-source (#6806)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 05:49:12 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.14.0
    to 2.14.2.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.14.0...v2.14.2)
    
    --- updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-version: 2.14.2
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time:joda-time in /data-prepper-plugins/s3-sink (#6804)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 1 May 2026 05:49:04 -0700
    
    
    Bumps [joda-time:joda-time](https://github.com/JodaOrg/joda-time) from 2.14.0
    to 2.14.2.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    - [Changelog](https://github.com/JodaOrg/joda-time/blob/main/RELEASE-NOTES.txt)
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.14.0...v2.14.2)
    
    --- updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-version: 2.14.2
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
    
    Signed-off-by: dependabot[bot] &lt;support@github.com&gt; Co-authored-by:
    dependabot[bot] &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Include child primary key in fields for one-to-one joins (#6794)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 30 Apr 2026 17:03:12 -0500
    
    
    For one-to-many joins, the child primary key (e.g., item_id) is excluded from
    the _fields metadata because the Painless script adds it separately via the
    child_pk_name parameter. However, for one-to-one joins, the script only
    iterates _fields to flatten child columns at the document root — it does not
    add the child primary key separately. This caused the child PK
    (e.g., shipping_id) to be missing from the denormalized OpenSearch document.
    
    Fix: Only exclude child primary key from _fields for one-to-many joins. For
    one-to-one joins, the child PK is now included in _fields so it gets flattened
    at the document root like other child columns.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Adds Xun Zhang (Zhangxunmt) to the maintainers. (#6793)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 30 Apr 2026 14:47:24 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __feat(otlp-sink): Add support for metrics and logs with configurable SigV4 and headers (#6768)__

    [Laszlo Kovacs](mailto:67709312+LaszloKovacs9001@users.noreply.github.com) - Thu, 30 Apr 2026 14:00:49 -0700
    
    
    * feat(otlp-sink): Add support for metrics and logs with configurable SigV4 and
    headers
    
    Extend the OTLP sink plugin to support all three signal types (traces, metrics,
    logs) with full OTLP protocol encoding, configurable SigV4 signing, and custom
    HTTP headers. Based on the work from @viquer in opensearch-project#6488, with
    additional enhancements.
    
    Multi-signal support:
    - Extended OTelProtoCodec with convertToResourceMetrics() and
     convertToResourceLogs() for encoding all signal types
    - Added generic OtlpSignalHandler&lt;T&gt; interface with type-safe
     implementations for traces, metrics, and logs
    - Per-signal-type buffer architecture for optimal batching
    - Signal type determined automatically at runtime from event type
    - Generalized sink from Record&lt;Span&gt; to Record&lt;Event&gt;
    
    Configurable SigV4 signing:
    - Added service_name field to OtlpSinkConfig (default: xray)
    - SigV4Signer uses configurable service name
    
    Additional headers:
    - Added additional_headers map config to OtlpSinkConfig
    - Protected header blocklist prevents overriding signed headers
    - Headers injected after SigV4 signing (not included in signature)
    
    Per-signal metrics:
    - Added per-signal counters (rejectedTracesCount, failedMetricsCount, etc.)
    - Aggregate counters (rejectedRecordsCount, failedRecordsCount) retained
    
    Region from AWS config:
    - getAwsRegion() checks aws.region first, falls back to endpoint parsing
    
    Breaking changes (plugin is @Experimental):
    - OtlpSink changed from AbstractSink&lt;Record&lt;Span&gt;&gt; to
     AbstractSink&lt;Record&lt;Event&gt;&gt;
    - Metric counters renamed: rejectedSpansCount to rejectedRecordsCount,
     failedSpansCount to failedRecordsCount
    
    Signed-off-by: Roberto Ramirez Vique &lt;viquer@amazon.com&gt; Signed-off-by: Laszlo
    Kovacs &lt;laszlokv@amazon.com&gt;
    
    * fix(otlp-sink): Address code review findings
    
    Fix EventHandle leak when encodeEvent() returns null or throws in 
    OtlpSinkBuffer.runTyped() — release handle in both paths.
    
    Fix SeverityNumber.forNumber() null safety in OTelProtoStandardCodec to prevent
    NPE on unrecognized severity numbers.
    
    Cache per-signal metric counters at construction time in OtlpSinkMetrics to
    avoid string concatenation and counter lookup on every increment call in the
    hot path.
    
    Remove stale OTLP_PATH constant and fallback URI from SigV4Signer since
    endpoint is a required config field.
    
    Replace fully-qualified class names with imports in OTelProtoCodec 
    OTelProtoEncoder interface (ResourceMetrics, ResourceLogs, Log).
    
    Update README: replace stale flush-on-signal-change batching description with
    accurate per-signal buffer architecture, add service_name to configuration
    options table.
    
    Replace Thread.sleep(300) with Awaitility in OtlpHttpSenderTest for 
    deterministic async assertions.
    
    Replace OutOfMemoryError with Error in OtlpSinkBufferMultiSignalTest to avoid
    triggering JVM OOM handlers in CI.
    
    Signed-off-by: Laszlo Kovacs &lt;laszlokv@amazon.com&gt;
    
    * fix(otlp-sink): Keep rejectedSpansCount/failedSpansCount metric names
    
    Rename TRACE metrics label from &#39;Traces&#39; to &#39;Spans&#39; to preserve backwards
    compatibility with existing metric allowlists and dashboards. Per-signal
    counters now emit rejectedSpansCount, rejectedMetricsCount, rejectedLogsCount
    (and corresponding failed* counters).
    
    Remove stale migration note from README since the original metric names are
    preserved.
    
    Signed-off-by: Laszlo Kovacs &lt;laszlokv@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Roberto Ramirez Vique &lt;viquer@amazon.com&gt; Signed-off-by: Laszlo
    Kovacs &lt;laszlokv@amazon.com&gt;

* __Convert Kafka Header values to Strings (#6507)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Thu, 30 Apr 2026 11:46:58 -0700
    
    
    * Convert Kafka Header values to Strings
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed header value parsing logic
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments. Created a new class for header extraction
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fixed license header failures
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Add SINGLE_SCAN discovery mode to OpenSearch source (#6169) (#6783)__

    [Keyur Patel](mailto:keyurpatel.opensource@gmail.com) - Thu, 30 Apr 2026 13:27:06 -0500
    
    
    Adds an optional discovery_mode setting to the OpenSearch source scheduling
    configuration. When set to SINGLE_SCAN, the source runs index discovery exactly
    once and completes each index partition terminally instead of closing it with a
    reopen interval. This avoids re-ingesting indices from the start when source
    coordinator state
    (e.g. DynamoDB item TTL) expires during long-running pipelines.
    
    PERIODIC remains the default and preserves existing behavior.
    
    Issue #6169
    
    Signed-off-by: Keyur-S-Patel &lt;keyurpatel.opensource@gmail.com&gt;

* __Remove engechas from active maintainers (#6791)__

    [Chase](mailto:62891993+engechas@users.noreply.github.com) - Wed, 29 Apr 2026 14:57:52 -0700
    
    
    Remove engechas from active maintainers
    
    Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt; Signed-off-by: David
    Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Adds Divyansh Bokadia (divbok) to the maintainers group. (#6789)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 29 Apr 2026 13:31:35 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the triage meeting time to 11am Central / 9am Pacific. (#6792)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 29 Apr 2026 13:03:18 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update Armeria to 1.32.6 (#6343)__

    [Jannik Brand](mailto:jannik.brand@sap.com) - Wed, 29 Apr 2026 19:09:05 +0200
    
    
    This Armeria patch version was requested explicitly: 
    https://github.com/line/armeria/issues/6733
    
    Fixes #6271 since the current Ameria version is affected by a couple of bugs
    (see comment
    https://github.com/opensearch-project/data-prepper/issues/6271#issuecomment-3627395389).
    
    Furthermore, replace io.micrometer.core.lang annotation which is going to be
    deprecated in Micrometer 1.16.0 - see release notes: 
    https://github.com/micrometer-metrics/micrometer/releases/tag/v1.16.0.
    
    Signed-off-by: Jannik Brand &lt;jannik.brand@sap.com&gt;

* __Minify Painless script before sending to OpenSearch (#6785)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 28 Apr 2026 21:13:15 -0500
    
    
    Strip comment lines and blank lines from the Painless script source at 
    initialization time. This reduces the script payload size sent to OpenSearch on
    every bulk request, improving network efficiency.
    
    The minification is done once in the ScriptManager constructor and the result
    is reused for all subsequent buildScript calls. Only full-line comments (lines
    starting with //) and empty lines are removed. Inline comments after code are
    preserved.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Reject external versioning when script is configured in OpenSearch sink (#6773)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 28 Apr 2026 19:08:04 -0500
    
    
    OpenSearch does not support external versioning with scripted upserts. Add
    config-time validation in IndexConfiguration to fail fast with a clear error
    message instead of getting runtime 400 errors from OpenSearch.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix RDS joins: set workers=1 on main pipeline to prevent event reordering (#6784)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Tue, 28 Apr 2026 19:07:36 -0500
    
    
    The joins template inherited the user-configured workers count (default 2) for
    the main pipeline. With workers &gt; 1, multiple threads write to the S3 sink
    concurrently. The S3 sink&#39;s ReentrantLock serializes writes but does not
    guarantee ordering — thread 2 can write item2 before thread 1 writes item1 for
    the same parent document.
    
    When the S3 sub-pipeline reads these out-of-order events and sends them to
    OpenSearch, the per-table version check in the Painless script rejects the
    lower-versioned item (noop), causing data loss for 1:N child records.
    
    Setting workers=1 ensures events are written to S3 in binlog order. This has no
    throughput impact since the S3 sink&#39;s ReentrantLock already serializes writes
    to a single thread at a time.
    
    Tested with 200 threads, 5M orders: 0 failures with workers=1 vs
    ~0.09% failure rate with workers=2.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Update OpenTelemetry javaagent to 2.26.1 in trace analytics sample app to fix CVE-2026-33701. (#6765)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 27 Apr 2026 07:23:00 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Support denormalized document joins for RDS source (#6762)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 23 Apr 2026 15:32:46 -0500
    
    
    * Add join configuration model and metadata enricher for RDS source
    
    Add JoinConfig, JoinRelation configuration classes and JoinMetadataEnricher 
    that enriches CDC events with join metadata (_table, _fields, _is_delete,
    _primary_key) to enable denormalization on write in the OpenSearch sink.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Wire join metadata enrichment into RDS source event pipeline
    
    Add joins config to RdsSourceConfig. Update RecordConverter.convert() to accept
    columnNames and call JoinMetadataEnricher when table participates in a join.
    Wire enricher creation in BinlogEventListener and
    LogicalReplicationEventProcessor.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add rds-joins template with auto-configured denormalization script
    
    Add rds-joins-rule.yaml that matches when both source.rds and source.rds.joins
    are present. Add rds-joins-template.yaml that configures the OpenSearch sink
    with upsert action and a painless script that selectively merges/removes fields
    based on join metadata.
    
    Update RuleEvaluator to sort rules by specificity (most apply_when conditions
    first) so the more specific rds-joins rule matches before the generic rds rule.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add 1:N join support with nested array denormalization
    
    Update JoinRelation to include child_primary_key for array element 
    identification. Update JoinMetadataEnricher to set _is_parent,
    _child_table_name, _child_pk_name, _child_pk_value metadata and exclude join
    key columns from _fields. Update painless script in rds-joins template to
    handle parent flat merge, child nested array insert/update/delete, and parent
    delete as full document delete.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Override S3 partition key for join tables to use parent key
    
    For child tables in a join, override the S3 partition key to use the join
    primary key (parent key value) instead of the child table&#39;s own primary key.
    This ensures related parent and child events hash to the same S3 folder so they
    are processed together by the s3 source pipeline.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Remove external versioning from joins template
    
    Multiple tables write to the same document in join mode. Events from different
    tables in the same transaction can share the same timestamp, causing version
    conflicts with external versioning since it requires strictly greater versions.
    The script itself is idempotent so versioning is not needed for correctness.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add username/password passthrough to rds-joins template
    
    Pass through username and password from customer&#39;s OpenSearch sink config to
    support basic auth in addition to AWS IAM auth.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add per-row versioning, join_type, max_child_records, and monotonic
    versioning for RDS join
    
    Changes:
    - Painless script: per-table versioning for parent/1:1, per-row versioning for
    1:N children
    - Configurable version_field (default __versions) to avoid field name
    collisions
    - join_type: one_to_one (flat merge) and one_to_many (array with
    max_child_records cap)
    - Monotonic version counter in StreamRecordConverter using AtomicLong
     (timestamp_millis * 1000 + sequence) for unique versions within same second
    - retryOnConflict(3) on scripted upsert bulk operations
    - Export path: wire JoinMetadataEnricher into DataFileScheduler, pass column
    names
    - Set default empty string for _child_pk_value on parent events to prevent NPE
    
    Tested: parent/child CRUD, 1:1 join, 1:N with max cap, child-before-parent, 
    concurrent writes, rapid updates, delete+re-insert, NULL values, special chars, 
    bulk UPDATE, REPLACE INTO, load tests (300+ events verified against MySQL).
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add composite FK support and FK change detection for RDS join
    
    Composite FK (multi-column join key):
    - JoinRelation: parent_key, child_key, child_primary_key changed from String to
    List&lt;String&gt;
     with ACCEPT_SINGLE_VALUE_AS_ARRAY for backward compatibility
    - JoinMetadataEnricher: composite key values joined with | for document ID and
    child PK
    - Painless script: pkNames split by |, composite matching in removeIf and trim
    cleanup
    
    FK change detection (before-image):
    - BinlogEventListener.handleUpdateEvent: detects when child FK columns change
     between before-image and after-image, emits DELETE for old parent doc
    - Supports composite FK: checks all key columns, triggers on any column change
    - RecordConverter: added getJoinMetadataEnricher() getter
    - JoinMetadataEnricher: added getChildKeyColumns() returning List&lt;String&gt;
    
    Requires binlog_row_image=FULL (Aurora MySQL default) for before-image
    availability.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add overlay directive for template transformer and update joins template
    
    Template transformer:
    - Added &lt;&lt;overlay path&gt;&gt; directive support in DynamicConfigTransformer
    - Processes after placeholder resolution, before model conversion
    - Supports [*] wildcard to apply overlay to all matching array elements
    - Deep merge semantics: overlay fields override target, nested objects merged
    - Example: &lt;&lt;overlay sink[*].opensearch&gt;&gt; merges join script into all OS sinks
    
    Joins template:
    - Changed from hardcoded OpenSearch sink to full customer sink passthrough
    - sink: &lt;&lt;$.&lt;&lt;pipeline-name&gt;&gt;.sink&gt;&gt; preserves all customer sink config
     (hosts, aws, index, dlq, routes, etc.)
    - &lt;&lt;overlay sink[*].opensearch&gt;&gt; injects action, document_id, script,
     scripted_upsert, retry_on_conflict into every OpenSearch sink entry
    - Non-OpenSearch sinks are left untouched
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Address PR review comments from @oeyh
    
    - Use Objects.equals() for FK comparison to handle NULL values
    (BinlogEventListener)
    - Create JoinType enum (ONE_TO_ONE, ONE_TO_MANY) and use in JoinRelation
    - Add proper ArrayList import in RuleEvaluator
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add unit tests for join metadata enricher, version counter, rule evaluator,
    and overlay directive
    
    - JoinMetadataEnricherTest: parent/child enrichment, composite keys,
     1:1 join type, delete events, isJoinTable, getChildKeyColumns
    - StreamRecordConverterTest: monotonic version counter (same millis
     increments, new millis resets, always &gt; export version)
    - RuleEvaluatorTest: more specific rule (2 conditions) matches before
     generic rule (1 condition) regardless of load order
    - DynamicConfigTransformerTest: overlay directive merges into opensearch
     sinks, leaves non-opensearch sinks untouched, removes overlay key
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Correct JacksonEvent.merge() null-key handling and close InputSt… (#6775)__

    [Siqi Ding](mailto:dingdd@amazon.com) - Thu, 23 Apr 2026 12:43:20 -0700
    
    
    Fix JacksonEvent.merge() null-key bug and InputStream leak
    
    Harden XML parser in LastReleasedVersionProvider against XXE attacks.
    
    Signed-off-by: Siqi Ding &lt;dingdd@amazon.com&gt;

* __Support shared catalog config across tables in iceberg-source (#6727)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 21 Apr 2026 10:30:40 -0700
    
    
    Support shared catalog config across tables in iceberg-source
    
    Change catalog fallback check from isEmpty() to null check so that an
    explicitly empty catalog is not silently treated as unset. TableConfig.catalog
    default is now null instead of emptyMap.
    
    Add IcebergServiceTest to verify catalog selection logic: shared only, table
    override, and mixed configurations.
    
    Replace fully qualified TestEventFactory references with imports in
    IcebergSourceIT.
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Enable kafka sink integration tests as a github workflow action (#6751)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Tue, 21 Apr 2026 12:28:42 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Add top-level filters option for S3 source (#6735)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Tue, 21 Apr 2026 06:49:56 -0700
    
    
    Add top-level filters option for S3 source
    
    Add a top-level filters configuration to the S3 source that applies 
    include_prefix and exclude_suffix filtering for both SQS and scan modes.
    
    Previously, key path filters were only available under scan bucket options,
    making it impossible to filter S3 objects when using SQS notifications. The new
    filters option uses the same bucket name keyed Map pattern as bucket_owners.
    
    Top-level filters and scan bucket-level filters cannot be used together, as the
    top-level filters are intended to eventually replace the scan bucket-level
    filters.
    
     Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Add Prometheus scrape/pull source to prometheus plugin [Adding in the same Prometheus Remote write source]  (#6743)__

    [Srikanth Padakanti](mailto:srikanth29.9@gmail.com) - Mon, 20 Apr 2026 19:22:15 -0700
    
    
    Add Prometheus scrape/pull source to prometheus plugin
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add configurable sort options to opensearch source search_options (#6761)__

    [Srikanth Padakanti](mailto:srikanth29.9@gmail.com) - Mon, 20 Apr 2026 18:45:00 -0700
    
    
    Add configurable sort options to opensearch source search_options
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt; 
    Co-authored-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add filter_list processor to filter list elements (#6659)__

    [yavmanis](mailto:yavmanis@amazon.com) - Mon, 20 Apr 2026 10:47:06 -0700
    
    
    Add filter_list processor to filter list elements
    
    - Add processor-scoped metadata:
     - filter_list_processor_failed_elements_count
     - filter_list_processor_failed_elements
    - Keep per-element filtering behavior unchanged
    - Add TODO for future optimization of evaluation path overhead
    - Rename source → iterate_on, keep_when → keep_element_when
    - Update FilterListProcessorConfig fields/getters/validation
    - Update FilterListProcessor to use new getters
    - Add SENSITIVE marker to warning logs
    - Add TODO(#6609) reference in processor
    - Document /value for primitive elements in keep_element_when
    - Update README examples and configuration docs
    - Update all tests and YAML fixtures to new key names
    - Verified: :data-prepper-plugins:mutate-event-processors:test (PASS)
    
    Signed-off-by: Manisha Yadav &lt;yavmanis@amazon.com&gt; Signed-off-by:
    nishantKadivar &lt;nimahesx@amazon.com&gt; Co-authored-by: nishantKadivar
    &lt;nimahesx@amazon.com&gt;

* __Fix invalid document version events still included in bulk requests (#6601) (#6758)__

    [Keyur Patel](mailto:keyurpatel.opensource@gmail.com) - Fri, 17 Apr 2026 19:51:29 -0500
    
    
    Events with invalid document_version values are correctly sent to the DLQ but
    were still being added to the OpenSearch bulk request. Added continue 
    statements after the NumberFormatException and RuntimeException catch blocks in
    doOutput() to skip the rest of the loop iteration for failed events.
    
    Made version conflict handling in BulkRetryStrategy conditional on external 
    versioning. Version conflicts are only silently handled (skipping 
    documentErrors) when document_version_type is set to external or external_gte.
    For internal versioning or when unset, version conflicts are treated as
    document errors.
    
    BulkRetryStrategy now accepts a boolean isExternalVersioning parameter instead
    of importing VersionType directly, keeping the OpenSearch client type out of
    its API. The VersionType check is done in OpenSearchSink via the
    isExternalVersionType helper method.
    
    Signed-off-by: Keyur-S-Patel &lt;keyurpatel.opensource@gmail.com&gt;

* __Add configurable metric_timestamp_source, metric_timestamp_granularity, stable host ID, and NodeOperationDetail dedup for APM service map processor (#6672) (#6672)__

    [Vamsi Manohar](mailto:reddyvam@amazon.com) - Fri, 17 Apr 2026 09:55:13 -0700
    
    
    Signed-off-by: Vamsi Manohar &lt;reddyvam@amazon.com&gt;

* __feat: add support for now() function evaluation and comparison (#6529)__

    [Leila Moussa](mailto:leila.farah.moussa@gmail.com) - Thu, 16 Apr 2026 09:51:40 -0700
    
    
    Signed-off-by: LeilaMoussa &lt;leila.farah.moussa@gmail.com&gt;

* __Add support for parent event field access during iterate_on processing in add_entries processor (#6713)__

    [yavmanis](mailto:yavmanis@amazon.com) - Thu, 16 Apr 2026 01:57:15 -0700
    
    
    Add disable_root_keys and evaluate_when_on_element configs to add_entries
    processor
    
    Signed-off-by: Manisha Yadav &lt;yavmanis@amazon.com&gt;

* __Default OpenSearch source serverless search context to point_in_time (#6756)__

    [Srikanth Padakanti](mailto:srikanth29.9@gmail.com) - Wed, 15 Apr 2026 08:57:01 -0700
    
    
    Signed-off-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt; 
    Co-authored-by: Srikanth Padakanti &lt;srikanth_padakanti@apple.com&gt;

* __Add wrap_entries processor to mutate-event-processors (#6665)__

    [nishantKadivar](mailto:nimahesx@amazon.com) - Tue, 14 Apr 2026 11:05:40 -0700
    
    
    * Add map_entries processor to mutate-event-processors
    
    Adds a new map_entries processor that wraps each element of a primitive array
    into an object using a configured key name. This enables downstream processors
    like add_entries and delete_entries with iterate_on, which require List of Map
    and cannot operate on primitive arrays.
    
    Example: [&#34;alice&#34;, &#34;bob&#34;] -&gt; [{&#34;name&#34;: &#34;alice&#34;}, {&#34;name&#34;: &#34;bob&#34;}]
    
    Configuration options:
    - source (required): key of the primitive array to transform
    - target (optional): key to write result to (defaults to source)
    - key (required): key name in each resulting object
    - exclude_null_empty_values: filter out null/empty elements
    - append_if_target_exists: append to existing target array
    - map_entries_when: conditional expression
    - tags_on_failure: tags on processing failure
    
    Includes 21 unit tests and 3 config tests.
    
    Signed-off-by: Nishant Kadivar &lt;nimahesx@amazon.com&gt;
    
    * fix(map_entries): Improve edge case handling for missing keys and empty
    filtered results
    
    - Change missing source key from warn+tags_on_failure to silent debug
     log, consistent with other mutate-event processors
    - Remove early return when all elements are filtered out by
     exclude_null_empty_values, ensuring consistent List&lt;Map&gt; output type
    - Update test and README to reflect new behavior
    
    Signed-off-by: Nishant Kadivar &lt;nimahesx@amazon.com&gt;
    
    * Refactor map_entries validation into config and add integration test
    
    Signed-off-by: Nishant Kadivar &lt;nimahesx@amazon.com&gt;
    
    * Add chained wrap_entries processor test for nested listsgit
    
    Signed-off-by: Nishant Kadivar &lt;nimahesx@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Nishant Kadivar &lt;nimahesx@amazon.com&gt;

* __Enable HTTP Sink with aws sigv4 auth mode only (#6747)__

    [Krishna Kondaka](mailto:krishkdk@amazon.com) - Tue, 14 Apr 2026 09:30:44 -0700
    
    
    * Enable HTTP Sink with aws sigv4 auth mode only
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;
    
    ---------
    
    Signed-off-by: Kondaka &lt;krishkdk@amazon.com&gt;

* __Refactor getBulkOperationForAction into BulkOperationFactory (#6748)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 13 Apr 2026 12:58:55 -0500
    
    
    * Refactor getBulkOperationForAction into BulkOperationFactory
    
    Extract bulk operation creation logic from OpenSearchSink into a dedicated
    BulkOperationFactory class for improved testability and separation of concerns.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Add BulkOperationFactory unit tests and update script tests
    
    Add BulkOperationFactoryTest covering create, index, update, upsert, delete,
    default action, optional fields, document filters, and pipeline setting. Update
    OpenSearchSinkScriptTest to test BulkOperationFactory directly instead of going
    through OpenSearchSink.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix script to use filtered document when document filters are active
    
    Pass filteredJsonNode instead of jsonNode to scriptManager.buildScript so that
    params.doc in the script reflects the post-filter document, consistent with how
    builder.document() and builder.upsert() use the filtered version.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Add support for zero dates in mysql mapping (#6750)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Fri, 10 Apr 2026 16:57:21 -0500
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __Drop Kinesis records with invalid UTF-8 bytes (#6746)__

    [Souvik Bose](mailto:souvik04in@gmail.com) - Fri, 10 Apr 2026 11:59:12 -0700
    
    
    Wrap codec.parse() in a try-catch in KinesisRecordConverter to handle parse
    failures (e.g. invalid UTF-8 surrogates) gracefully. Failed records are logged
    and skipped instead of crashing the pipeline. Adds a recordParseErrors metric
    counter via PluginMetrics.
    
    Signed-off-by: Souvik Bose &lt;souvbose@amazon.com&gt; Co-authored-by: Souvik Bose
    &lt;souvbose@amazon.com&gt;

* __chore(deps): update dependency werkzeug to v3.1.6 (#6562)__

    [mend-for-github-com[bot]](mailto:50673670+mend-for-github-com[bot]@users.noreply.github.com) - Fri, 10 Apr 2026 09:05:25 -0700
    
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Automate creation of the changelog after the release build. After the verification and tagging this will create a PR with a changelog between tags. This also updates some plugin versions used in the release GitHub Action to the latest versions. (#6742)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 10 Apr 2026 08:25:46 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add generic script support for update/upsert operations in OpenSearch sink (#6744)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Thu, 9 Apr 2026 22:31:23 -0500
    
    
    * Add generic script support for update/upsert operations in OpenSearch sink
    
    Adds the ability to configure a script on the OpenSearch sink that gets applied
    to update and upsert bulk operations. This is a generic mechanism that passes
    script source and params through to OpenSearch&#39;s bulk API. Script language is
    hardcoded to painless. The event document is automatically passed as
    params.doc. scripted_upsert is always true so the script runs on every write
    including the first create. Script param values support ${} expression syntax
    for dynamic resolution from event fields or metadata.
    
    Configuration example:
     sink:
       - opensearch:
           hosts: [&#34;https://localhost:9200&#34;]
           index: &#34;my-index&#34;
           action: &#34;upsert&#34;
           document_id: &#34;${/id}&#34;
           script:
             source: &#34;ctx._source.putAll(params.doc); ctx._source.source =
    params.table&#34;
             params:
               table: &#34;${getMetadata(\&#34;table_name\&#34;)}&#34;
    
    Resolves #3563
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Fix checkstyle unused imports and update compressed bulk request size
    expectations
    
    Remove unused imports (ScriptConfiguration, HashMap, Map) from OpenSearchSink.
    Update expected compressed bulk request sizes in integration tests to account
    for the additional resolvedScriptParameters field in SerializedJsonImpl.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Fix flaky PipelinesWithAcksIT by awaiting ack callback (#6718)__

    [Sotaro Hikita](mailto:70102274+lawofcycles@users.noreply.github.com) - Thu, 9 Apr 2026 13:00:39 -0700
    
    
    The acknowledgement callback is invoked asynchronously after sink processing
    completes. Tests were asserting the ack result immediately after confirming
    sink output, causing intermittent NullPointerException when the callback had
    not yet fired.
    
    Wrap ack assertions in await().untilAsserted() to poll until the callback
    completes, matching the pattern already used in 
    three_pipelines_with_all_unrouted_records().
    
    Signed-off-by: Sotaro Hikita &lt;bering1814@gmail.com&gt;

* __Passing http request headers as metadata in the event for http source (#6671)__

    [Divyansh Bokadia](mailto:dbokadia@amazon.com) - Wed, 8 Apr 2026 10:31:02 -0700
    
    
    Signed-off-by: Divyansh Bokadia &lt;dbokadia@amazon.com&gt;

* __When kafka consumer metric is NaN, report previous value instead of invalid value (#6741)__

    [Taylor Gray](mailto:tylgry@amazon.com) - Wed, 8 Apr 2026 11:24:13 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updates the RELEASING.md to for changelog generation (#6739)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 7 Apr 2026 11:08:37 -0700
    
    
    Updates the RELEASING.md to put the changelog generation after the build and
    using the tags.
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.15.0 changelog (#6736)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 14:53:13 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix MongoTasksRefresher to force executor restart on MongoSecurityException (#6716)__

    [Dinu John](mailto:86094133+dinujoh@users.noreply.github.com) - Mon, 6 Apr 2026 14:58:15 -0500
    
    
    * Fix MongoTasksRefresher to force executor restart on MongoSecurityException
    
    When DocumentDB revokes old credentials after secret rotation, the pipeline 
    enters a permanent auth failure state because basicAuthChanged() returns false
    (the secret value in Secrets Manager hasn&#39;t changed).
    
    Add forceRefresh() to MongoTasksRefresher with exponential backoff (30s, 60s, 
    120s, max 3 attempts) that restarts the executor with the current config.
    
    StreamScheduler now walks the exception cause chain for MongoSecurityException 
    and calls forceRefresh() when detected. If all 3 attempts fail, falls back to 
    the normal hourly scheduled credential refresh.
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    * Remove unused imports in mongodb plugin tests
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;
    
    ---------
    
    Signed-off-by: Dinu John &lt;86094133+dinujoh@users.noreply.github.com&gt;

* __Adds prompts for creating Data Prepper release notes using AI tools. This allows us to use AI to create the release notes consistently between versions and across different maintainers. They are geared toward Claude but should work for tools like Kiro. (#6732)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 11:05:38 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Data Prepper 2.15.0 release notes. (#6731)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 11:02:50 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update the RELEASING.md file for the newest steps based on recent scripts and GitHub Actions. (#6730)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 10:40:21 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the next Data Prepper release to 2.16 now that the 2.15 branch has been made. (#6729)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 10:40:09 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Run older OpenSearch 2.x integration tests against Ubuntu 22 to fix cgroup failure causing test failures. (#6717)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 6 Apr 2026 09:44:59 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;


