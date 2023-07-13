
* __Added 2.3.1 change log (#2872) (#2905)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 12 Jul 2023 11:39:01 -0500
    
    * Added 2.3.1 change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated change log
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 0e1aa457de25ee2de712db6f0c7c7316587b92b7)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Generated THIRD-PARTY file for 8fb9d79 (#3014)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 12 Jul 2023 09:46:13 -0500
    
    
    Signed-off-by: GitHub &lt;noreply@github.com&gt;
    Co-authored-by: dlvenable
    &lt;dlvenable@users.noreply.github.com&gt;

* __Updated to Data Prepper 2.3.2 for release. (#3013)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 12 Jul 2023 09:45:57 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Fix bucket ownership validation. Resolves #3005 (#3009) (#3011)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 12 Jul 2023 09:13:29 -0500
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    (cherry picked from commit decccb9a82e01cbaa059ce32d781d8614c494111)
     Co-authored-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove validation that made keys starting or ending with . - or _ inv… (#3007)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 12 Jul 2023 09:12:49 -0500
    
    
    * Remove validation that made keys starting or ending with . - or _ invalid,
    catch all exceptions in the parse json processor (#2945)
     Remove validation that made keys starting or ending with . - or _ invalid,
    catch all exceptions in the parse json processor
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 05d229a06ceddb21cd9dedcaf49b1d455272fe6f)
    
    * Removed readme
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    Co-authored-by:
    Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix race condition in SqsWorker when acknowledgements are enabled (#3001) (#3010)__

    [kkondaka](mailto:41027584+kkondaka@users.noreply.github.com) - Tue, 11 Jul 2023 22:11:10 -0700
    
    
    * Fix race condition in SqsWorker when acknowledgements are enabled
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Modified to do the synchronization in the acknowledgement set framework
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Removed unused variable
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comment and fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Addressed review comments
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed failing tests
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    * Fixed checkStyle failure
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    
    ---------
     Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 515cf6114f5270f5f4fc94eba6bdd62e45659944)

* __Retry s3 reads on socket exceptions. (#2992) (#3008)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Jul 2023 16:55:29 -0500
    
    
    * Retry s3 reads on socket exceptions.
     S3 will reset the conenction on their end frequently. To not lose data,
    data
    prepper should retry all socket exceptions by attempting to re-open
    the
    stream.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    
    * Bubble up parquet exceptions.
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    
    ---------
     Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    (cherry picked from commit 9f78542533dd24ed21e29a12950938c0c4b23636)
     Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Fix S3 errors around end of file behavior. (#2983) (#3006)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Jul 2023 15:36:47 -0500
    
    
    Signed-off-by: Adi Suresh &lt;adsuresh@amazon.com&gt;
    (cherry picked from commit 75fa735289ecf8d335d5681fa63a512e8a4ee03e)
     Co-authored-by: Adi Suresh &lt;adsuresh@amazon.com&gt;

* __Fix SqsWorker error messages (#2991) (#3002)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Jul 2023 12:57:30 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 45b6e554fdad117396b5cc3bbcf52b7b99b42a5a)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Fix CVE-2023-35165, CVE-2023-34455, CVE-2023-34453, CVE-2023-34454, C… (#2948) (#2952)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 11 Jul 2023 14:33:01 -0500
    
    
    * Fix CVE-2023-35165, CVE-2023-34455, CVE-2023-34453, CVE-2023-34454,
    CVE-2023-2976
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    * Updated snappy version in build.gradle files
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    
    ---------
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 8e2145cc4c00fb2a93b97a3fcdb689609e23ff63)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix DLQ writer writing empty list (#2931) (#2998)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 10 Jul 2023 09:46:24 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 1dd8bd385fd61fbb269e2eca17bca431189060bf)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Fix addTags API in EventMetadata (#2926) (#2996)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 10 Jul 2023 09:41:29 -0700
    
    
    Signed-off-by: Krishna Kondaka
    &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    Co-authored-by:
    Krishna Kondaka &lt;krishkdk@dev-dsk-krishkdk-2c-bd29c437.us-west-2.amazon.com&gt;
    (cherry picked from commit 5565337ec07281f8b58f65e275185112811357c0)
     Co-authored-by: kkondaka &lt;41027584+kkondaka@users.noreply.github.com&gt;

* __Updated the release date (#2911) (#2912)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 20 Jun 2023 13:45:59 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
    (cherry picked from commit 7649059824b286dfb714ed78ab12c9c42e53ff92)
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;


