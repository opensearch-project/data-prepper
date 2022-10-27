
* __Reduce smoke test timeout to 8 minutes from 30 minutes. These tests tend to pass within 3 minutes in my personal GitHub branch. So this leaves quite a bit of buffer time. It helps speed up retrying failures from flaky tests. (#1956) (#1965)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 24 Oct 2022 09:36:23 -0500
    
    EAD -&gt; refs/heads/2.0, tag: refs/tags/2.0.1, refs/remotes/origin/2.0
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 8977667a925691f1f8584ff04fbf739cfa26689b)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Attempt to reduce flakiness in RandomStringSourceTests by using awaitility. Split tests into two. JUnit 5. (#1921) (#1964)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 24 Oct 2022 09:36:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 3d641f767fa4e75d9d7cfdc0ac88a336c81f30a1)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Run smoke tests against OpenSearch 1.3.6. (#1955) (#1962)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 21 Oct 2022 08:33:01 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 43b7d33c28cb8572a92c397d59e90a3d9aba6ebb)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Python grpcio 1.50.0 in smoke tests to reduce time to run. (#1954) (#1959)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 21 Oct 2022 08:32:44 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit c6e0f6c50227ec62b90a75bd5dca33af252f70d3)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Delete s3:TestEvent objects and log them when they are found in the SQS queue. Resolves #1924. (#1939) (#1953)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 20 Oct 2022 13:30:57 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 8600d93f74b2ffd46cde58a3c5d65b81f3ded459)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Add ExecutorService to DataPrepperServer (#1948) (#1951)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 20 Oct 2022 12:59:13 -0500
    
    
    * Add ExecutorService to DataPrepperServer
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    
    * Shutdown executor service after stopping server
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
     Signed-off-by: Chase Engelbrecht &lt;engechas@amazon.com&gt;
    (cherry picked from commit cc53e6fd31a2bd02024cd5250375b0cc9407711e)
     Signed-off-by: Chase &lt;62891993+engechas@users.noreply.github.com&gt;
     Co-authored-by: Chase &lt;62891993+engechas@users.noreply.github.com&gt;

* __Updated the THIRD-PARTY for 2.0.1 (#1950)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 20 Oct 2022 12:02:08 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Require protobuf-java-util 3.21.7 to fix #1891 (#1938) (#1945)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 19 Oct 2022 15:27:39 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 029b3e7e8c7354c41818f1dca32c06c9116d3342)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Version bump: Data Prepper 2.0.1 (#1937)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 19 Oct 2022 12:36:33 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bug Fix: S3 source key  (#1926) (#1942)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 19 Oct 2022 12:22:59 -0500
    
    
    * Fix: S3 source key bug fix
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 11cd1129a53471a383774f11f1565690abf0d9d5)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Jackson 2.13.4.2 (#1925) (#1933)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 19 Oct 2022 11:38:24 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 941f808abeda545cdbe44d8548e38ee65633bda8)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix PipelineConnector to duplicate the events (#1897) (#1918)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 12 Oct 2022 20:27:07 -0500
    
    
    * Fix string mutate processors to duplicate the events
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Fix string mutate processors to duplicate the events - made changes as per
    David&#39;s suggestions
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Removed unnecessary changes leftover from 1st commit
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Modified PipelineConnector to duplicate JacksonSpan type events too. Added
    testcases in PipelineConnectorTest
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added a new testcase for JacksonSpan withData()
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * Addressed review comment and added parallel pipeline test to github/workflows
    
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    
    * fixed workflow failure
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
     Signed-off-by: Krishna Kondaka &lt;krishkdk@amazon.com&gt;
    Co-authored-by: Krishna
    Kondaka &lt;krishkdk@amazon.com&gt;
    (cherry picked from commit 5bd7a31b02f6e316c865b49b24cd719555ab51f0)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;
    
    Signed-off-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updated the release notes for 2.0.0 (#1911) (#1913)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Oct 2022 14:20:30 -0500
    
    
    Updated the release notes for 2.0.0
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 892162ae34eabf049769a5ef80f553a53a5d0462)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the change log for 2.0.0 with most recent changes. (#1909) (#1912)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Oct 2022 14:20:16 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit 202c16fcef5dbe818c87a89c7d585b2b34d0559c)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Simple duration regex did not allow for 0s or 0ms (#1910) (#1917)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Oct 2022 14:20:03 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 932ea00589c95c35f59e2ed7cbe4358f5f90bef5)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;


