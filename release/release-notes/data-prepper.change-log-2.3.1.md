
* __Do not suppress logs when there are exception in s3 source. (#2896) (#2897)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Sat, 17 Jun 2023 10:35:30 -0500

  EAD -&gt; refs/heads/2.3, refs/remotes/upstream/2.3
  Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
  (cherry picked from commit 4ed1a3bb524af0a73673b1bf6c61c5d5e5fd0e0e)
  Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Change log for index name format failure in opensearch sink (#2894) (#2895)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Jun 2023 16:19:05 -0500


    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 83536e5fb1a3127644e9b055173e2a1bfa407011)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Updated s3 sink metrics (#2888) (#2892)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Jun 2023 09:58:57 -0500


    (cherry picked from commit 8e7114fb1260b70579fd006a81fe1883d187c049)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add exception when gzip input stream not have magic header. (#2879) (#2882)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 16 Jun 2023 09:28:13 -0500


    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    (cherry picked from commit bf22a4d99f69e37f6056ef11b1f719b423a7bd47)
     Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __FIX: concurrentModification (#2876) (#2880)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 15 Jun 2023 19:15:04 -0500


    (cherry picked from commit cb0471b3ea385a66ee76fc4bacd14c18e8addca1)
     Signed-off-by: Qi Chen &lt;qchea@amazon.com&gt;
    Signed-off-by: George Chen
    &lt;qchea@amazon.com&gt;
    Co-authored-by: Qi Chen &lt;qchea@amazon.com&gt;

* __Fix silent dropping of data when index format has null keys, write to dlq if configured (#2885) (#2886)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 15 Jun 2023 15:38:09 -0500


    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 98f7ce73416dcb13b28ffe8b1de4b405b4aa56a9)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Update README.md for S3 sink (#2878) (#2884)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 15 Jun 2023 10:47:46 -0500


    Signed-off-by: Travis Benedict &lt;benedtra@amazon.com&gt;
    (cherry picked from commit 6a8e165c5b2c4ab8da7b1c02c3f5329a44107d91)
     Co-authored-by: Travis Benedict &lt;benedtra@amazon.com&gt;

* __Generated THIRD-PARTY file for b352cb6 (#2870)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 13 Jun 2023 21:11:06 -0500


    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: asifsmohammed
    &lt;asifsmohammed@users.noreply.github.com&gt;

* __S3 EventBridge and security lake support (#2861) (#2868)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 13 Jun 2023 21:10:39 -0500


    * EventBridge initial working draft
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 040c23219767a078bf20835d87ff07c56c9079c6)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Add STS external ID to all STS configurations. (#2862) (#2867)__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Jun 2023 20:28:39 -0500


    STS external ID is required by some AWS services when making an STS
    AssumeRole
    call.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    (cherry picked from commit 674527f6ac53df772bce5bba09a4e6b5e8df873e)
     Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Update to next version 2.3.1 (#2865)__

  [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 13 Jun 2023 18:15:07 -0500


    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix CVE in maven-artifact by excluding that dependency (#2848) (#2863)__

  [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 13 Jun 2023 13:51:23 -0500


    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 3598ba4143a713c848b45cc4c73bbf547c5dc02c)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;


