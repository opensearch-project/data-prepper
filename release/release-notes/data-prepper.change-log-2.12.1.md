
* __Generated THIRD-PARTY file for d553432 (#5956)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 4 Aug 2025 14:10:08 -0700
    
    EAD -&gt; refs/heads/2.12, refs/remotes/upstream/2.12
    Signed-off-by: GitHub &lt;noreply@github.com&gt; Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Updates to Data Prepper 2.12.1 (#5954)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 4 Aug 2025 13:58:13 -0700
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates the Postgresql JDBC driver to 42.7.1 to fix CVE-2025-49146. (#5935) (#5936)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 1 Aug 2025 08:38:27 -0700
    
    
    (cherry picked from commit eac98b3031ecbb0c5c86d9b0b45e914743edb3ba)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Fix S3DBService and LocalDBService file overwrite handling during downloads (#5911) (#5934)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 31 Jul 2025 16:34:02 -0700
    
    
    Fix S3DBService/LocalDBService file overwrite handling during downloads
    
    
    
    (cherry picked from commit b87890ad33fd50fac700e21758825d651c8f33d9)
    
    Signed-off-by: kirtanhk &lt;kirtanhk@amazon.com&gt; Co-authored-by: Kirtan Kakadiya
    &lt;35823164+KirtanKakadiya@users.noreply.github.com&gt; Co-authored-by: kirtanhk
    &lt;kirtanhk@amazon.com&gt;

* __chore(deps): update dependency setuptools to v78 (#5727) (#5926)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 30 Jul 2025 12:45:06 -0700
    
    
    (cherry picked from commit aff23afca7be6eb37ce42d1448e445bc8b72b853)
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Update dependency aws-cdk-lib (#5886) (#5925)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 30 Jul 2025 12:44:45 -0700
    
    
    (cherry picked from commit fe0d8a71143ca5310452406353eb3551656374c5)
    
    Co-authored-by: mend-for-github-com[bot]
    &lt;50673670+mend-for-github-com[bot]@users.noreply.github.com&gt;

* __Bump armeria + grpc + protobuf to fix CVE-2024-7254 (#5891) (#5924)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 30 Jul 2025 12:44:26 -0700
    
    
    Bump armeria + grpc + protobuf to fix CVE-2024-7254
    
    Upgrades protobuf dependencies with versions that fix Fixes CVE-2024-7254.
    
    Use inline mocks in DnsPeerListProviderCreationTest to support mocking final
    classes. Updates to the GrpcRequestExceptionHandlerTest required by the update
    to the Armeria test library. Enforce a consistent JUnit version across the
    project to avoid JUnit consistency issues.
    
    
    
    
    (cherry picked from commit 292a547f61a549a57f173d7c899f7ca697e7b5df)
    
    Signed-off-by: Karsten Schnitter &lt;k.schnitter@sap.com&gt; Signed-off-by: David
    Venable &lt;dlv@amazon.com&gt; Co-authored-by: Karsten Schnitter
    &lt;k.schnitter@sap.com&gt; Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updates several dependencies to address CVEs (#5914) (#5923)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 30 Jul 2025 09:53:45 -0700
    
    
    Updates several dependencies to address CVEs
    
    * CVE-2025-46762 - Parquet 1.15.2
    * CVE-2025-48734 - commons-beanutils 1.11.0 and Checkstyle 10.26.1
    * CVE-2024-57699 - json-smart 2.5.2
    * CVE-2025-24970 - Netty 4.1.123
    * CVE-2025-27817 - Apache Kafka 3.9.1 and Confluent Kafka 7.9.1
    
    Also, removes some broken code related to the kafka-client in unused Kafka
    tests.
    
    
    (cherry picked from commit c8f66fa4fd1ed67fdbbeb230daf948e76207cf10)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Updated the smoke tests scripts to use the end-to-end tests (#5903) (#5913)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 29 Jul 2025 09:46:27 -0700
    
    
    Updated the smoke tests scripts to use the end-to-end tests to get them running
    as part of the release again. Support validating tar files by skipping the
    Docker pull if the image is local. Skip the Docker validations in the
    run-smoke-tests.sh script altogether to rely on the e2e-tests. Increase
    validation time to 30 minutes.
    
    
    (cherry picked from commit c6f072aa7ff695cdafd6994e0cb959da401c2bb3)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;

* __Fixes a regression in core where @SingleThread annotated processors are only running the last instance. (#5902) (#5904)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 28 Jul 2025 09:54:09 -0700
    
    
    Fixes a regression in core where @SingleThread annotated processors are only
    running the last instance. Also, disable the ProcessorSwapPipelineIT test since
    this feature is not yet completed.
    
    Fixes #5901
    
    
    (cherry picked from commit 53f16d786616a19d52d6b998d18d93f3008508f8)
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt; Co-authored-by: David Venable
    &lt;dlv@amazon.com&gt;


