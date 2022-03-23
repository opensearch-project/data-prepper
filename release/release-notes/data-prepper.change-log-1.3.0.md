## 2022-03-22 Version 1.3.0

---

* __Fix bug where a group can be concluded twice in the Aggregate Processor (#1229) (#1230)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 21 Mar 2022 14:40:35 -0500
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit cbf1082c88acab85f6d7dbac71c6cd6f5932a8d0)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Fix incorrect key-value documentation (#1222) (#1225)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 18 Mar 2022 17:05:17 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;
    (cherry picked from commit 3ad35e973f3ccf268dfb90066030b3bb47d0bafd)
     Co-authored-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Smoke test tar (#1200) (#1219)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 18 Mar 2022 14:10:36 -0500
    
    
    * Added tar smoke test
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    (cherry picked from commit c684f7ca036357566e46bd83dd477bec9b185bcf)
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Add in clarification sentence (#1208) (#1212)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 18 Mar 2022 14:07:49 -0500
    
    
    * Add in clarification sentence
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    (cherry picked from commit 63a35eb39b0f3fd2bfebef4e04242a0dfa9820ac)
     Co-authored-by: David Powers &lt;37314042+dapowers87@users.noreply.github.com&gt;

* __Fixed broken links (#1205) (#1211)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 17 Mar 2022 12:57:06 -0500
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    (cherry picked from commit 3980c626dc076bfaf2c28e22d8b1da18f3efc8e9)
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __FIX: remove extra quotes in string literal (#1207) (#1209)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 16 Mar 2022 16:44:36 -0500
    
    
    * FIX: remove extra quotes in string literal
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: ParseTreeCoercionServiceTest
     Signed-off-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    (cherry picked from commit 46a08d975ea192be09cf27907b2afa4c939ed288)
     Co-authored-by: Qi Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix checkstyle error (#1203) (#1206)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 16 Mar 2022 15:32:24 -0500
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    (cherry picked from commit b93ce866d75176db956e4cfa0073a65ff7ac7f08)
     Co-authored-by: Shivani Shukla &lt;67481911+sshivanii@users.noreply.github.com&gt;
    
    Signed-off-by: Shivani Shukla &lt;67481911+sshivanii@users.noreply.github.com&gt;

* __Updated gradle version to 1.3.0 (#1204)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 16 Mar 2022 15:12:33 -0500
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Maintenance: 1.3.0 changelog and release notes (#1201)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 16 Mar 2022 11:08:01 -0500
    
    
    * MAINT: changelog and release notes
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * Nit: highlight pipeline definition change
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: merge duplicate item
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: add DI
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: merged conditional into drop processor
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: add README for all features
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __add in single char delimiter config (#1202)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Wed, 16 Mar 2022 10:51:50 -0500
    
    
    * add in single char delimiter config
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Added instructions for releasing Data Prepper (#1198)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Mar 2022 10:01:54 -0500
    
    
    Updated the release README.md with instructions on performing the release
    process. Moved the smoke test details to a new README.md in the smoke-tests
    directory.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Refactor mutate mapper (#1199)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 15 Mar 2022 17:38:20 -0500
    
    
    * Refactor MutateMapper
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __README for Mutate String Processors (#1191)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 15 Mar 2022 17:35:21 -0500
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Data Prepper Expression Package Integration Test (#1166)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Tue, 15 Mar 2022 11:50:56 -0500
    
    efs/heads/BlogBio
    * Added DI test cases
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Fix Split Processor Constructor (#1196)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 15 Mar 2022 11:48:18 -0500
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Maintenance: JacksonSpan enhancement (#1197)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 15 Mar 2022 11:38:14 -0500
    
    
    * MAINT: remove string format
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ENH: checkAndSetDefaultValues in builder
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: add test cases to achieve coverage
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __FIX: temporarily allow output generic object type (#1192)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 15 Mar 2022 09:44:38 -0500
    
    
    Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Force snakeyaml to version 1.29 (#1193)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Mon, 14 Mar 2022 18:56:24 -0500
    
    
    Force snakeyaml to version 1.29 (#1193)
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Upload Maven artifacts as part of Release build (#1181)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 14 Mar 2022 14:15:36 -0500
    
    
    Gradle task to upload Maven artifacts to the staging S3 bucket. Changed the
    Maven publish plugin to publish to the root build directory. Fixed the root
    clean task to actually delete the build directory. Updated the Release GitHub
    Action to upload Maven artifacts.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the Developer Guide with instructions for contributing code. (#1168)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 14 Mar 2022 14:14:33 -0500
    
    
    Updated the Developer Guide with instructions for contributing code. This
    documents our approach to branches and backports.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Upgrade to latest jackson-databind version (#1183)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 14 Mar 2022 13:37:57 -0500
    
    
    Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Equal operator fix (#1177)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Mon, 14 Mar 2022 13:33:26 -0500
    
    
    * Added complex numeric comparison to equals operator
    * Handles integers, floats, null, objects and any combination of.
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Drop when only (#1174)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Mon, 14 Mar 2022 09:42:35 -0500
    
    
    * Added DropEventProcessorConfig
    * Added DropEventsWhenCondition
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __FIX: coercion on json pointer values (#1178)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Fri, 11 Mar 2022 17:13:20 -0600
    
    
    * FIX: coercion on jsonpointer values
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: compactify type checking
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused import
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ENH: use bean injection instead of static variable
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused import
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Split String Processor (#1167)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Fri, 11 Mar 2022 17:07:44 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Substitute processor (#1173)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Fri, 11 Mar 2022 16:02:13 -0600
    
    
    * Add `SubstituteStringProcessor` and its associated tests
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __New operators (#1172)__

    [Steven Bayer](mailto:smbayer@amazon.com) - Fri, 11 Mar 2022 14:28:59 -0600
    
    
    * Added subtract unary numeric operator
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Updated the Release GitHub Actions workflow to perform a smoke test on the Docker image. Renamed the job. (#1175)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 11 Mar 2022 11:27:35 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Feature: parse tree evaulator and listener (#1169)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 10 Mar 2022 15:45:15 -0600
    
    
    * ADD: ParseTreeCoercionService and tests
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ENH: add rule index in operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ADD: OperatorProvider to encapsulate operator retrieval
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: reuse nodeStringValue
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address all PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: test case name
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use more generic interface
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: parameterized tests on escape json pointer
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: fix test cases
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: separate variable
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ADD: ParseTreeEvaluator and ParseTreeEvaluatorListener
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: javadoc
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: visit error node
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * FIX: more conflicts
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: unused imports
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ENH: expression coercion exception runtime
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Add drop_events.drop_when documentation (#1164)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 10 Mar 2022 14:59:06 -0600
    
    
    * Added expression syntax documentation
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Negative value support with unary subtract operator (#1165)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 10 Mar 2022 14:41:03 -0600
    
    
    * Added SUBTRACT unary operators
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added data-prepper-logstash-configuration to jacocoTestCoverageVerification (#1163)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 10 Mar 2022 14:31:45 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Lower test coverage (#1171)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 10 Mar 2022 11:00:10 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __When core (#1157)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 9 Mar 2022 14:06:48 -0600
    
    
    * ContextManager scans org.opensearch.dataprepper.expression
    * Added ParseTreeParserConfiguration
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Enhancement: rule index in operators and OperatorProvider (#1155)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 9 Mar 2022 13:33:41 -0600
    
    
    * ENH: add rule index in operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * ADD: OperatorProvider to encapsulate operator retrieval
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address all PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use more generic interface
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Feature: ParseTreeCoercionService and tests (#1153)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 9 Mar 2022 13:33:13 -0600
    
    
    * ADD: ParseTreeCoercionService and tests
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: reuse nodeStringValue
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: test case name
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * TST: parameterized tests on escape json pointer
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: fix test cases
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: separate variable
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Added data-prepper-expression to test coverage report (#1162)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 9 Mar 2022 12:46:35 -0600
    
    
    * Added data-prepper-expression to test coverage report
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Updated the release build to push the Docker image to ECR and upload archives to S3. (#1151)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Mar 2022 08:53:41 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the Backport workflow and added a workflow to delete backport branches. #692 (#1159)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Mar 2022 16:20:09 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Reintroduce `WithKeysConfig` (#1156)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 8 Mar 2022 15:06:49 -0600
    
    
    * Reintroduce `WithKeysConfig`
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Lowercase and trim processors (#1147)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 8 Mar 2022 10:50:25 -0600
    
    
    * Add UppercaseStringProcessor and its associated base classes and config
    classes
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Fix not bug (#1144)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 7 Mar 2022 11:30:07 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added support for nested syntax in converter (#1088)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sun, 6 Mar 2022 00:12:50 -0600
    
    
    * Added support for nested syntax in converter
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added spring dependency injection support for plugins (#1140)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 4 Mar 2022 14:33:17 -0600
    
    
    * Added Spring DI for plugins support
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Added unit tests for PluginArgumentsContext DI

* __Updates NoOpPrepper to implement Processor (#1142)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Fri, 4 Mar 2022 12:25:36 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    

* __Created a CDK stack to create a staging artifacts bucket. (#1141)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 4 Mar 2022 11:31:47 -0600
    
    
    Created a CDK stack to create a staging artifacts bucket. Improved the CDK
    README to document deploying the whole stack.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add UppercaseStringProcessor (#1137)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Fri, 4 Mar 2022 11:29:12 -0600
    
    
    * Add UppercaseStringProcessor and its associated base classes and config
    classes
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Staging CDK: Allow releasing to the staging repository from any branch. (#1143)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 4 Mar 2022 10:34:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump spring-core from 5.3.15 to 5.3.16 in /data-prepper-core (#1113)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 21:28:00 -0600
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.15 to 5.3.16.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.15...v5.3.16)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.15 to 5.3.16 in /data-prepper-core (#1114)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 21:13:50 -0600
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.15 to 5.3.16.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.15...v5.3.16)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.15 to 5.3.16 in /data-prepper-core (#1115)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 21:13:25 -0600
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.15 to 5.3.16.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.15...v5.3.16)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Support default values for attributes (#1095)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Thu, 3 Mar 2022 16:34:24 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Feature: operators in data-prepper expression (#1065)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 3 Mar 2022 10:53:51 -0600
    
    
    * FEAT: operators and tests
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: lower -&gt; less
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use int for symbol
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: include null in equality operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: operator classes package private
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: eval -&gt; evaluate
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: simplify numerical value comparison operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: DI annotation on operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: OperatorFactory to produce negate and numeric comparison operator
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: use NumericCompareOperator
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: generic operator to cover negate
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: generic operators
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: static final
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MNT: Objects::equals
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Verify against OpenSearch 1.2.4 during the CI integration tests. (#1133)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 2 Mar 2022 15:57:55 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated gradle tasks to remove zip files from tasks (#1132)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 2 Mar 2022 10:45:21 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Implemented ConditionalExpressionEvaluator methods (#1128)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 2 Mar 2022 10:26:47 -0600
    
    
    * Implemented ConditionalExpressionEvaluator methods
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Created a GitHub Action to check the Staging Resources CDK project. (#1111)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 2 Mar 2022 10:18:47 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Example: Kubernetes-Fluent-Bit-Data-Prepper demo setup (#729)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 2 Mar 2022 09:59:47 -0600
    
    
    * EXP: kubernetes-fluentbit-data-prepper-setup
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: refine doc
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: a concrete command on running data-prepper
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: partially address the PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: more accurate description and reference
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: add debug step
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: specify minikube version
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: doc name
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: space
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Updated the THIRD-PARTY file with the removal of Jakarta EL. (#1134)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 1 Mar 2022 16:34:11 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Delete outdated Kibana trace analytics example (#1135)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 1 Mar 2022 14:51:05 -0600
    
    
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Bump log4j-bom from 2.17.1 to 2.17.2 in /data-prepper-core (#1117)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 1 Mar 2022 12:22:37 -0600
    
    
    Bumps log4j-bom from 2.17.1 to 2.17.2.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.logging.log4j:log4j-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Added Tests for complete expression parsing (#1107)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 1 Mar 2022 12:03:49 -0600
    
    
    * Added Tests for complete expression parsing
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Use Hibernate&#39;s ParameterMessageInterpolator which allows us to remove the Jakarta EL library and its license. (#1127)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 1 Mar 2022 11:52:21 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Netty 4.1.74, which fixes CVE-2021-43797. (#1126)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 1 Mar 2022 11:04:28 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump guava in /data-prepper-plugins/otel-trace-raw-prepper (#1112)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 1 Mar 2022 11:04:06 -0600
    
    
    Bumps [guava](https://github.com/google/guava) from 31.0.1-jre to 31.1-jre.
    - [Release notes](https://github.com/google/guava/releases)
    - [Commits](https://github.com/google/guava/commits)
    
    ---
    updated-dependencies:
    - dependency-name: com.google.guava:guava
     dependency-type: direct:production
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated THIRD PARTY file. (#1125)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 1 Mar 2022 10:10:21 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Staging resources CDK: Include CloudFront distribution and release IAM role (#1099)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Feb 2022 16:35:19 -0600
    
    
    Updated the staging resources CDK with a CloudFront distribution and the IAM
    role for GitHub Actions to assume to perform the release process. This includes
    the necessary S3 bucket policy for the CloudFront origin access identity.
    Updated the README.md with instructions on build and deployment.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated JUnit Jupiter to 5.8.2 (#1108)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Feb 2022 14:06:30 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Upload release archive files to a unique key path per build. Use a default value of &#39;development&#39; to make it easier to continue to upload archive files from local. (#1100)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Feb 2022 12:57:14 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated Gradle projects to use the dependency string notation. It can help with find all commands. (#1106)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 28 Feb 2022 12:56:56 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added ParseTreeTest (#1096)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 28 Feb 2022 11:43:35 -0600
    
    
    * Added ParseTreeTest
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Parentheses expression matcher (#1097)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 28 Feb 2022 11:38:28 -0600
    
    
    * Added parentheses expression matcher
    * Added ParenthesesExpressionMatcher JavaDoc
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Renamed CompositeException (#1101)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 28 Feb 2022 11:04:49 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Update Open Distro usages to OpenSearch in scripts (#1086)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Mon, 28 Feb 2022 09:57:10 -0600
    
    
    * Removed ODFE usage
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;
    
    * Updated odfe usage to opensearch
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Updated to OpenSearch from odfe
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Renamed ODFE to OpenDistro
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Added ODFE reference for users
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;
    
    * Update ODFE to ES
    Signed-off-by: Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Added ParseTreeParser (#1090)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 28 Feb 2022 09:36:14 -0600
    
    
    * Added ParseTreeParser
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added LiteralMatcher (#1092)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 25 Feb 2022 15:13:16 -0600
    
    
    * Added LiteralMatcher
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added JsonPointerMatcher (#1091)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 25 Feb 2022 13:13:43 -0600
    
    
    * Added JsonPointerMatcher
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Created a CDK app to deploy staging resources for the release process (#1094)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 25 Feb 2022 13:01:11 -0600
    
    
    Created a CDK app to deploy staging resources for the Data Prepper release
    process. It currently deploys an ECS repository and OpenID Connect access for
    GitHub.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added standard duration string deserializer for Data Prepper plugin, refactored the AggregateProcessor group_duration to use this deserializer (#1093)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 25 Feb 2022 12:13:53 -0600
    
    
    Added standard duration string deserializer for Data Prepper plugin, refactored
    the AggregateProcessor group_duration to use this deserializer
     Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added SimpleExpressionMatcher (#1073)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 24 Feb 2022 14:53:58 -0600
    
    
    * Added SimpleExpressionMatcher
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Fix: added missing configuration in yaml mapping (#1089)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Thu, 24 Feb 2022 14:00:36 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Error parser tests (#1085)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 24 Feb 2022 11:28:44 -0600
    
    
    * Added error listener test
    * Added negative parser tests
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    

* __Bugfix: mismatched conf and yaml file names (#1087)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 23 Feb 2022 17:23:28 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added README.md for date processor (#1059)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 23 Feb 2022 12:00:10 -0600
    
    
    * Added README.md for date processor
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added ErrorListener (#1084)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 23 Feb 2022 11:57:32 -0600
    
    
    Added ErrorListener
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Mutate logstash convert (#1042)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Wed, 23 Feb 2022 11:08:36 -0600
    
    
    * Add in Logstash Conversion for the Mutate Object processors
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Added ParseRuleContextExceptionMatcherTest (#1071)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 22 Feb 2022 12:03:35 -0600
    
    
    * Added ParseRuleContextExceptionMatcherTest
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Bug fix: updated formatter and access modifier (#1070)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 22 Feb 2022 11:31:19 -0600
    
    
    * Bug fix: changed the access modifier of constructor
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __OpenSearch Index DateTime Pattern conversion (#1045)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Tue, 22 Feb 2022 10:26:54 -0600
    
    
    * Added AttributesMapper for OpenSearchPlugin index
    Signed-off-by: Shivani
    Shukla &lt;sshkamz@amazon.com&gt;

* __Publish to Maven as part of the release build. Updated existing step names for release build. (#1064)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 21 Feb 2022 14:34:59 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __removed dead code (#1072)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 21 Feb 2022 10:38:40 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Update fromMessage API in data-prepper-common Sources (#1074)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Mon, 21 Feb 2022 10:05:22 -0600
    
    
    * Updated StdIn, RandomStringSource to use fromMessage API
    Signed-off-by:
    Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __updated grammar (#1069)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 18 Feb 2022 17:12:52 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added metrics and destination timezone configuration option. (#1052)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 18 Feb 2022 15:11:26 -0600
    
    
    * Added metrics and destination timezone configuration option.
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Date processor logstash converter (#1031)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 18 Feb 2022 14:26:57 -0600
    
    
    * Added logstash converter for date processor
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix for broken link to monitoring page and heading formats. (#1060)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Fri, 18 Feb 2022 12:43:46 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump armeria-junit5 in /data-prepper-plugins/armeria-common (#967)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 17 Feb 2022 10:50:55 -0600
    
    
    Bumps [armeria-junit5](https://github.com/line/armeria) from 1.13.4 to 1.14.0.
    - [Release notes](https://github.com/line/armeria/releases)
    - [Changelog](https://github.com/line/armeria/blob/master/.post-release-msg)
    -
    [Commits](https://github.com/line/armeria/compare/armeria-1.13.4...armeria-1.14.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.linecorp.armeria:armeria-junit5
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump bcprov-jdk15on in /data-prepper-plugins/otel-trace-source (#796)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 17 Feb 2022 10:50:24 -0600
    
    
    Bumps [bcprov-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcprov-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump byte-buddy in /data-prepper-plugins/opensearch (#1033)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 17 Feb 2022 10:50:00 -0600
    
    
    Bumps [byte-buddy](https://github.com/raphw/byte-buddy) from 1.11.20 to 1.12.8.
    
    - [Release notes](https://github.com/raphw/byte-buddy/releases)
    - [Changelog](https://github.com/raphw/byte-buddy/blob/master/release-notes.md)
    
    -
    [Commits](https://github.com/raphw/byte-buddy/compare/byte-buddy-1.11.20...byte-buddy-1.12.8)
    
    
    ---
    updated-dependencies:
    - dependency-name: net.bytebuddy:byte-buddy
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump aws-java-sdk-core in /data-prepper-plugins/opensearch (#1035)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Thu, 17 Feb 2022 10:46:48 -0600
    
    
    Bumps [aws-java-sdk-core](https://github.com/aws/aws-sdk-java) from 1.12.67 to
    1.12.159.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.67...1.12.159)
    
    ---
    updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Remove the macOS build from the distributions build since Data Prepper is currently not building a macOS distribution. (#1047)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 16 Feb 2022 17:36:10 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add simple integration tests for AggregateProcessor (#1046)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 16 Feb 2022 15:40:57 -0600
    
    
    Add simple integration tests for AggregateProcessor
     Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added testing utils for testing antlr generated parser (#1037)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Wed, 16 Feb 2022 09:11:58 -0600
    
    
    * Added Interfaces to define StatementEvaluator API for external packages
    * Added parser test with mock token stream
    * Added first parser test
    * Added MockTokenStreamHelper
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Updated docker image location (#1039)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 15 Feb 2022 23:18:42 -0600
    
    
    * Updated docker image location and run command
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Fix README headers (#1040)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 15 Feb 2022 15:57:52 -0600
    
    
    * Fix README headers
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Added Date processor (#1014)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 15 Feb 2022 13:36:11 -0600
    
    
    * Added date processor
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump spring-core from 5.3.13 to 5.3.15 in /data-prepper-core (#911)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 15 Feb 2022 09:27:22 -0600
    
    
    Bumps [spring-core](https://github.com/spring-projects/spring-framework) from
    5.3.13 to 5.3.15.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.13...v5.3.15)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump snakeyaml from 1.29 to 1.30 (#965)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 15 Feb 2022 09:24:56 -0600
    
    
    Bumps [snakeyaml](https://bitbucket.org/snakeyaml/snakeyaml) from 1.29 to 1.30.
    
    -
    [Commits](https://bitbucket.org/snakeyaml/snakeyaml/branches/compare/snakeyaml-1.30..snakeyaml-1.29)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.yaml:snakeyaml
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump awaitility from 4.1.0 to 4.1.1 (#1017)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Tue, 15 Feb 2022 09:24:35 -0600
    
    
    Bumps [awaitility](https://github.com/awaitility/awaitility) from 4.1.0 to
    4.1.1.
    - [Release notes](https://github.com/awaitility/awaitility/releases)
    -
    [Changelog](https://github.com/awaitility/awaitility/blob/master/changelog.txt)
    
    -
    [Commits](https://github.com/awaitility/awaitility/compare/awaitility-4.1.0...awaitility-4.1.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.awaitility:awaitility
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Lexer tests (#1030)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 14 Feb 2022 17:47:41 -0600
    
    
    * Added lexer test
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Remove Gradle permissions change for GitHub Actions workflows. (#1028)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 14 Feb 2022 11:27:43 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Mutate Entry Processors (#1002)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Mon, 14 Feb 2022 10:02:17 -0600
    
    
    Add Generic Mutate Processors
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Updated ConditionalExpressionEvaluator scope to package private (#1029)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 11 Feb 2022 17:11:45 -0600
    
    
    Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added Interfaces to define StatementEvaluator API for external packages (#1027)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 11 Feb 2022 15:58:44 -0600
    
    
    * Added Interfaces to define StatementEvaluator API for external packages
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Initial GitHub Action to build all artifacts. (#1023)__

    [David Venable](mailto:dlv@amazon.com) - Fri, 11 Feb 2022 14:50:34 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add documentation for the AggregateProcessor, change window_duration to group_duration (#1026)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 11 Feb 2022 11:47:25 -0600
    
    
    Add documentation for the AggregateProcessor, change window_duration to
    group_duration
     Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added Data Prepper Expression Grammar (#1024)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 11 Feb 2022 10:41:38 -0600
    
    
    * Added Data Prepper Expression Grammar
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Added metrics for the Aggregate Processor (#1022)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 10 Feb 2022 12:10:34 -0600
    
    
    Added metrics for the Aggregate Processor
     Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Fixed issue where Spring was unable to find the PrometheusMeterRegistry Bean (#1019)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Wed, 9 Feb 2022 11:53:54 -0600
    
    
    Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Bump aws-java-sdk-bom from 1.12.67 to 1.12.155 (#1010)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 9 Feb 2022 10:21:44 -0600
    
    
    Bumps [aws-java-sdk-bom](https://github.com/aws/aws-sdk-java) from 1.12.67 to
    1.12.155.
    - [Release notes](https://github.com/aws/aws-sdk-java/releases)
    - [Changelog](https://github.com/aws/aws-sdk-java/blob/master/CHANGELOG.md)
    - [Commits](https://github.com/aws/aws-sdk-java/compare/1.12.67...1.12.155)
    
    ---
    updated-dependencies:
    - dependency-name: com.amazonaws:aws-java-sdk-bom
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump httpcore from 4.4.13 to 4.4.15 (#964)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 9 Feb 2022 10:20:55 -0600
    
    
    Bumps httpcore from 4.4.13 to 4.4.15.
    
    ---
    updated-dependencies:
    - dependency-name: org.apache.httpcomponents:httpcore
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated Gson to 2.8.9. This dependency comes in through protobuf-java-util, so I updated it to 3.19.4, but that still uses the older version of Gson. So the core build needs to require 2.8.9. (#1012)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Feb 2022 10:11:58 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated to Gatling 3.7.4 (#1013)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 9 Feb 2022 09:59:16 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump slf4j-api from 1.7.32 to 1.7.36 in /data-prepper-plugins/opensearch (#1009)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 9 Feb 2022 09:45:19 -0600
    
    
    Bumps [slf4j-api](https://github.com/qos-ch/slf4j) from 1.7.32 to 1.7.36.
    - [Release notes](https://github.com/qos-ch/slf4j/releases)
    - [Commits](https://github.com/qos-ch/slf4j/compare/v_1.7.32...v_1.7.36)
    
    ---
    updated-dependencies:
    - dependency-name: org.slf4j:slf4j-api
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated Jackson to 2.13.1. (#1006)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 8 Feb 2022 16:48:35 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Change CombineAggregateAction name to PutAllAggregateAction (#1000)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 7 Feb 2022 16:59:39 -0600
    
    efs/heads/MetricServerBug
    Signed-off-by: graytaylor0 &lt;tylgry@amazon.com&gt;

* __Added concludeGroup logic to AggregateProcessor, added AggregateActionSynchronizer class to synchronize groupConcluding and eventHandling (#969)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 7 Feb 2022 13:35:05 -0600
    
    
    Added concludeGroup logic to AggregateProcessor, added
    AggregateActionSynchronizer class to synchronize groupConcluding and
    eventHandling
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Minor correction to the --signoff parameter in the CONTRIBUTING.md file. (#982)__

    [David Venable](mailto:dlv@amazon.com) - Sun, 6 Feb 2022 16:06:53 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added DateProcessor boilerplate and  DateProcessorconfig (#971)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Tue, 1 Feb 2022 18:44:28 -0600
    
    
    * Added boilerplate and configuration for Date Processor
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added performance test project (#955)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 1 Feb 2022 12:44:44 -0600
    
    
    * Added performance test project
    * Added performance test compile github action
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __[BUG] Fixed some failing unit tests on Windows (#968)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 1 Feb 2022 12:43:17 -0600
    
    
    * Fixed handful of failing unit tests on Windows
     Signed-off-by: Taylor Gray &lt;tylgry@amazon.com&gt;

* __Support loading plugins from multiple packages. (#948)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 31 Jan 2022 13:16:05 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Deprecating Record and RecordMetaData (#954)__

    [Mohit Saxena](mailto:76725454+mohitsaxenaknoldus@users.noreply.github.com) - Sat, 29 Jan 2022 15:29:14 -0600
    
    
    Signed-off-by: Mohit Saxena &lt;mohit.saxena@knoldus.com&gt;

* __Added custom hamcrest matcher for maps (#952)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Sat, 29 Jan 2022 11:35:02 -0600
    
    
    * Added custom matcher for maps in data-prepper-test-common
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Refactored by AggregateActionInput and GroupState interfaces, and class DefaultGroupState (#945)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 27 Jan 2022 15:46:48 -0600
    
    
    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Load AggregateAction, implement AggregateAction.handleEvent in AggregateProcessor, add GroupStateManager and AggregateIdentificationKeysHasher classes for use in AggregateProcessor (#931)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 25 Jan 2022 11:57:11 -0600
    
    
    Load AggregateAction, implement AggregateAction.handleEvent in
    AggregateProcessor, add AggregateGroupManager, AggregateGroup, and
    AggregateIdentificationKeysHasher classes for use in AggregateProcessor
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Create mapping yaml for KeyValueProcessor. Add in a few more tests (#922)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Mon, 24 Jan 2022 15:05:14 -0600
    
    
    Add KeyValueProcessor and its associated tests
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Updated copyright headers in scripts. #189 (#933)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Jan 2022 13:02:27 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the plugin framework to use dependency injection for internal classes. These changes remove testing constructors, primarily for DefaultPluginFactory. Along with this change, the integration tests load the application context to ensure that real objects are tested. (#934)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 24 Jan 2022 11:54:59 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#797)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 21 Jan 2022 20:40:48 -0600
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.6.0 to
    1.6.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.6.0...v1.6.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib-common in /data-prepper-plugins/mapdb-prepper-state (#741)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 21 Jan 2022 20:21:26 -0600
    
    
    Bumps [kotlin-stdlib-common](https://github.com/JetBrains/kotlin) from 1.5.31
    to 1.6.10.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/v1.6.10/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.31...v1.6.10)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib-common
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Di server (#846)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Fri, 21 Jan 2022 15:19:19 -0600
    
    
    * Added DataPrepper, DataPrepperServer now constructed by Spring DI
    * Refactored Http/Https server generation to separate provider class
    * Updated unit tests to use new constructors
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Updated remaining copyright headers in data-prepper (#928)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Jan 2022 21:42:01 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump bcpkix-jdk15on in /data-prepper-plugins/otel-trace-source (#795)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:49:09 -0600
    
    
    Bumps [bcpkix-jdk15on](https://github.com/bcgit/bc-java) from 1.69 to 1.70.
    - [Release notes](https://github.com/bcgit/bc-java/releases)
    -
    [Changelog](https://github.com/bcgit/bc-java/blob/master/docs/releasenotes.html)
    
    - [Commits](https://github.com/bcgit/bc-java/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.bouncycastle:bcpkix-jdk15on
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jakarta.el from 4.0.1 to 4.0.2 in /data-prepper-core (#905)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:48:42 -0600
    
    
    Bumps [jakarta.el](https://github.com/eclipse-ee4j/el-ri) from 4.0.1 to 4.0.2.
    - [Release notes](https://github.com/eclipse-ee4j/el-ri/releases)
    - [Commits](https://github.com/eclipse-ee4j/el-ri/commits)
    
    ---
    updated-dependencies:
    - dependency-name: org.glassfish:jakarta.el
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump armeria-junit5 in /data-prepper-plugins/armeria-common (#904)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:26:28 -0600
    
    
    Bumps [armeria-junit5](https://github.com/line/armeria) from 1.9.2 to 1.13.4.
    - [Release notes](https://github.com/line/armeria/releases)
    - [Changelog](https://github.com/line/armeria/blob/master/.post-release-msg)
    -
    [Commits](https://github.com/line/armeria/compare/armeria-1.9.2...armeria-1.13.4)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.linecorp.armeria:armeria-junit5
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-test from 5.3.14 to 5.3.15 in /data-prepper-core (#908)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:25:50 -0600
    
    
    Bumps [spring-test](https://github.com/spring-projects/spring-framework) from
    5.3.14 to 5.3.15.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.14...v5.3.15)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-test
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump protobuf-java-util in /data-prepper-plugins/otel-trace-raw-prepper (#910)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:24:48 -0600
    
    
    Bumps [protobuf-java-util](https://github.com/protocolbuffers/protobuf) from
    3.19.1 to 3.19.3.
    - [Release notes](https://github.com/protocolbuffers/protobuf/releases)
    -
    [Changelog](https://github.com/protocolbuffers/protobuf/blob/master/generate_changelog.py)
    
    -
    [Commits](https://github.com/protocolbuffers/protobuf/compare/v3.19.1...v3.19.3)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.google.protobuf:protobuf-java-util
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump joda-time from 2.10.12 to 2.10.13 (#923)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:23:20 -0600
    
    
    Bumps [joda-time](https://github.com/JodaOrg/joda-time) from 2.10.12 to
    2.10.13.
    - [Release notes](https://github.com/JodaOrg/joda-time/releases)
    -
    [Changelog](https://github.com/JodaOrg/joda-time/blob/master/RELEASE-NOTES.txt)
    
    - [Commits](https://github.com/JodaOrg/joda-time/compare/v2.10.12...v2.10.13)
    
    ---
    updated-dependencies:
    - dependency-name: joda-time:joda-time
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-core from 2.13.0 to 2.13.1 (#907)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 13:00:36 -0600
    
    
    Bumps [jackson-core](https://github.com/FasterXML/jackson-core) from 2.13.0 to
    2.13.1.
    - [Release notes](https://github.com/FasterXML/jackson-core/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-core/compare/jackson-core-2.13.0...jackson-core-2.13.1)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-core
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump spring-context from 5.3.13 to 5.3.15 in /data-prepper-core (#903)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 12:59:59 -0600
    
    
    Bumps [spring-context](https://github.com/spring-projects/spring-framework)
    from 5.3.13 to 5.3.15.
    - [Release notes](https://github.com/spring-projects/spring-framework/releases)
    
    -
    [Commits](https://github.com/spring-projects/spring-framework/compare/v5.3.13...v5.3.15)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.springframework:spring-context
     dependency-type: direct:production
     update-type: version-update:semver-patch
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-source (#902)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 12:59:25 -0600
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.21.0 to
    3.22.0.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.21.0...assertj-core-3.22.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump assertj-core in /data-prepper-plugins/otel-trace-raw-prepper (#901)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 12:58:59 -0600
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.21.0 to
    3.22.0.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.21.0...assertj-core-3.22.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Updated copyright headers in data-prepper-plugins package (#899)__

    [Asif Sohail Mohammed](mailto:nsifmoh@amazon.com) - Wed, 19 Jan 2022 09:51:12 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Bump assertj-core in /data-prepper-plugins/http-source (#900)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 19 Jan 2022 09:40:43 -0600
    
    
    Bumps [assertj-core](https://github.com/assertj/assertj-core) from 3.20.2 to
    3.22.0.
    - [Release notes](https://github.com/assertj/assertj-core/releases)
    -
    [Commits](https://github.com/assertj/assertj-core/compare/assertj-core-3.20.2...assertj-core-3.22.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: org.assertj:assertj-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __adding dependabot configs for all the new processors (#879)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 18 Jan 2022 19:00:47 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Kv processor (#872)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 18 Jan 2022 11:17:29 -0600
    
    
    * Added KeyValueProcessor
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __adding treddeni-amazon to maintainers (#881)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 18 Jan 2022 06:39:50 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Bump jackson-databind from 2.13.0 to 2.13.1 in /data-prepper-api (#790)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Fri, 14 Jan 2022 18:08:08 +0000
    
    
    

* __updating maintainers list (#873)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 13 Jan 2022 14:19:28 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Bring the log-ingest example pipeline back to using prepper. This is compatible with both 1.2 and 1.3 and thus gives more flexibility for users. (#856)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 13 Jan 2022 10:19:34 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added AggregateAction interface and implementations for Combine and RemoveDuplicate Aggregate Actions (#850)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 13 Jan 2022 09:25:08 -0600
    
    
    Added AggregateAction interface, and implementations for Combine and
    RemoveDuplicates Aggregate Actions
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Updated the project to resolve plugins from Maven Central before JCenter. Ignore Gradle metadata in the main build because the Gradle License Report plugin is attempting to use the .module file. (#860)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 12 Jan 2022 16:08:52 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Upgrade docker-compose.yml files from ODFE to OpenSearch. (#847)__

    [Han Jiang](mailto:jianghan@amazon.com) - Mon, 10 Jan 2022 15:00:26 -0600
    
    
    * Upgrade docker-compose.yml files from ODFE to OpenSearch.
    Signed-off-by: Han
    Jiang &lt;jianghan@amazon.com&gt;
    
    * Remove an unused shell script.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;

* __Add Drop Mapping (#841)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Mon, 10 Jan 2022 12:59:20 -0600
    
    
    * Add Drop Mapping
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Add Event.getAsMap() function to return the Event as a Map&lt;Object, Object&gt; (#848)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 10 Jan 2022 11:12:03 -0600
    
    
    Add Event.toMap() function to return the Event as a Map&lt;String, Object&gt;
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Explain index&#39;s date-time pattern in Sink&#39;s README.md file. (#833)__

    [Han Jiang](mailto:jianghan@amazon.com) - Fri, 7 Jan 2022 10:51:31 -0600
    
    
    * Explain date-time pattern in Sink&#39;s README.md file.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Optimize index date-time pattern description.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Put strings in back ticks to be more visible.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;

* __Added AggregateProcessor boilerplate and AggregateProcessorConfig (#839)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 7 Jan 2022 10:11:33 -0600
    
    
    Added AggregateProcessor boilerplate and AggregateProcessorConfig
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Di core (#815)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Thu, 6 Jan 2022 15:17:46 -0600
    
    
    * Added Spring Core dependency
    * Converted DataPrepper, DataPrepperServer, and DefaultPluginFactory classes to
    @Named instances
    * Added Configuration Classes: DataPrepperAppConfiguration, MetricsConfig, and
    PipelineParserConfiguration to support @Named class dependencies.
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Fix Javadoc errors and add a Javadoc step to verify that the Javadocs remain valid. (#828)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Jan 2022 14:29:47 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated HTTPSourceConfig to use JSR-380 validation. Also removed the javax.validation package in favor of the jakarta.validation package in data-prepper-core. (#830)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 6 Jan 2022 13:58:32 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Add PipelineDescription object to be passed to @DataPrepperPluginConstructor (#825)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 6 Jan 2022 10:58:50 -0600
    
    
    Add PipelineDescription object to be passed to @DataPrepperPluginConstructor
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Validate plugin configuration objects using JSR-303/380 validation. Hibernate Validator provides this validation, removing the need for bval. (#826)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Jan 2022 13:38:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Use Java8&#39;s Base64 in ServiceMapRelationship. (#827)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Jan 2022 13:36:23 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Change &#39;prepper&#39; to &#39;processor&#39; (#817)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Wed, 5 Jan 2022 12:32:59 -0600
    
    
    Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Add .whitesource configuration file (#821)__

    [whitesource-for-github-com[bot]](mailto:50673670+whitesource-for-github-com[bot]@users.noreply.github.com) - Wed, 5 Jan 2022 12:16:59 -0600
    
    
    Co-authored-by: whitesource-for-github-com[bot]
    &lt;50673670+whitesource-for-github-com[bot]@users.noreply.github.com&gt;

* __Bump com.diffplug.spotless from 5.17.0 to 6.1.0 (#800)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 5 Jan 2022 11:26:55 -0600
    
    
    Bumps com.diffplug.spotless from 5.17.0 to 6.1.0.
    
    ---
    updated-dependencies:
    - dependency-name: com.diffplug.spotless
     dependency-type: direct:production
     update-type: version-update:semver-major
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Adds withMessageKey API to JacksonEvent builder (#770)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Wed, 5 Jan 2022 11:00:36 -0600
    
    
    * Adds fromMessage API to JacksonEvent builder
    Signed-off-by: Shivani Shukla
    &lt;sshkamz@amazon.com&gt;

* __Support time pattern in index names. (#788)__

    [Han Jiang](mailto:jianghan@amazon.com) - Wed, 5 Jan 2022 10:30:26 -0600
    
    
    * Support time pattern in index names.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Add validations for index time pattern
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Only allow one of time pattern in an index.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Correct the check on the count of time patterns.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;
    
    * Check on nested time patterns in index.
    Signed-off-by: Han Jiang
    &lt;jianghan@amazon.com&gt;

* __Assemble the data-prepper-core uber-jar using Zip64 to support more classes. (#820)__

    [David Venable](mailto:dlv@amazon.com) - Wed, 5 Jan 2022 10:27:38 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated to log4j 2.17.1 to fix CVE-2021-44832. (#804)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Jan 2022 10:44:18 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Removed the year from the copyright in the NOTICE file. (#803)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 4 Jan 2022 10:43:43 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Drop processor (#801)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 4 Jan 2022 10:05:41 -0600
    
    
    * Create Drop Processor and a few simple tests
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    

* __Updated copyright headers for all projects (#776)__

    [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Thu, 23 Dec 2021 13:26:09 -0600
    
    
    Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;
     Co-authored-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Added Logstash OpenSearch output plugin mapping to converter (#756)__

    [Asif Sohail Mohammed](mailto:mdasifsohail7@gmail.com) - Wed, 22 Dec 2021 16:17:16 -0600
    
    
    * Added support for negating boolean logstash attribute value
     Signed-off-by: Asif Sohail Mohammed &lt;nsifmoh@amazon.com&gt;

* __Maintenance: remove traceGroup keys from required (#775)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 21 Dec 2021 14:17:32 -0600
    
    
    Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Add AbstractProcessor constructor that takes a PluginMetrics (#773)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 21 Dec 2021 13:25:00 -0600
    
    
    Add AbstractProcessor constructor that takes a PluginMetrics
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Support smoke testing local docker images (#765)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 21 Dec 2021 11:27:02 -0600
    
    
    * Support smoke testing local docker images
    * Update smoke test readme doc
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Maintenance: span model enhancement (#768)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 21 Dec 2021 10:30:27 -0600
    
    efs/remotes/upstream/maint/546-migrate-trace-analytics-plugin
    * MAINT: add serviceName attribute
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: copy over span in JacksonSpan Builder
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: rewrite equal method and add test coverage
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * DOC: since 1.3
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Fix boolean conversion and integer conversion bug for Logstash configuration converter attributes (#766)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 20 Dec 2021 16:31:32 -0600
    
    
    Fix boolean conversion and integer conversion bug for Logstash configuration
    converter attributes
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Smoke test 1.2 (#702)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 20 Dec 2021 11:35:12 -0600
    
    
    * Added scripted smoke test for http source
    * Added smoke test documentation
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __Updated to log4j 2.17.0 which fixes CVE-2021-45105. Resolves #759 (#760)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 20 Dec 2021 10:55:10 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Maintenance: file sink to support Event model (#750)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 16 Dec 2021 17:10:52 -0600
    
    
    * MAINT: use generic type in FileSink
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * Nit: FileSink in RuntimeException
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: log wrong record type instead of throwing RuntimeException
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Maintenance: StringPrepper use Event model (#753)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Thu, 16 Dec 2021 15:44:56 -0600
    
    
    * MAINT: StringPrepper use Event model
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: Only convert string value
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: import
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: redundant type
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: java 8
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __Updated kotline-stdlib-common to 1.6.0 (#701)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Dec 2021 12:23:38 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated the ADOT example to use OpenSearch Data Prepper, OpenSearch, OpenSearch Dashboards. (#703)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Dec 2021 12:19:04 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Update log insgestion demo guide with newly released Docker image (#752)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Thu, 16 Dec 2021 11:33:50 -0600
    
    
    Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Removed duplicate imports from merge.__

    [David Venable](mailto:dlv@amazon.com) - Thu, 16 Dec 2021 10:46:25 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;
    

* __FIX: useBlockingExecutor in otel-trace-source (#745)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Wed, 15 Dec 2021 12:12:55 -0600
    
    
    * FIX: useBlockingExecutor and tests
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * STY: no wildcard
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __add log analytics to README.md (#698)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 14 Dec 2021 13:25:12 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Added backport GitHub workflow. #692 (#730)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Dec 2021 13:21:34 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Added release notes for Data Prepper 1.1.1 (#724) (#732)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Dec 2021 09:59:17 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated to log4j-core 2.16.0 (#740)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 14 Dec 2021 08:03:48 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Updated to log4j-core 2.15.0. Require this version from transitive dependencies. (#731)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 13 Dec 2021 20:21:11 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Remove regex validation for JacksonEvent checkKey and replace with custom validation (#725)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Mon, 13 Dec 2021 12:40:09 -0600
    
    
    Removed regex key validation and replaced it with custom ascii/character
    validation. Added max key length of 2048
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Change JacksonEvent isNumeric to StringUtils.isNumeric (#711)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Fri, 10 Dec 2021 13:28:18 -0600
    
    
    * Swapped JacksonEvent isNumeric with StringUtils.isNumeric
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Add yum update to data prepper to consume all pkg updates (#713)__

    [Peter Zhu](mailto:zhujiaxi@amazon.com) - Fri, 10 Dec 2021 13:56:24 -0500
    
    
    Signed-off-by: Peter Zhu &lt;zhujiaxi@amazon.com&gt;

* __Refactored event checkKey to use pre-compiled regex pattern. refactored RandomStringSource (#706)__

    [Taylor Gray](mailto:33740195+graytaylor0@users.noreply.github.com) - Tue, 7 Dec 2021 18:58:15 -0600
    
    
    * Refactored event `checkKey` to use pre-compiled regex pattern
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Example: ECS FireLens integration (#704)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 7 Dec 2021 16:36:23 -0600
    
    
    * EXAMPLE: ecs firelens grok pipeline
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: update README
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: address PR comments
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;

* __migrating demo sample application to use opensearch and opensearch da… (#666)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Fri, 3 Dec 2021 07:03:25 -0600
    
    
    migrating demo sample application to use opensearch and opensearch dashboards
    instead of ODFE and kibana
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Updated the Jaeger HotROD demo with correct link (#693)__

    [David Venable](mailto:dlv@amazon.com) - Thu, 2 Dec 2021 09:33:33 -0600
    
    
    Updated the Jaeger HotROD documentation to use the correct link for Trace
    Analytics in OpenSearch dashboards and to use the name HotROD consistently.
    Include the old link for Trace Analytics in OpenSearch 1.1.0 and below.
     Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __simplifying log ingestion guide to use a single node cluster (#695)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 2 Dec 2021 09:08:14 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Bump kotlin-stdlib in /data-prepper-plugins/mapdb-prepper-state (#682)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Dec 2021 09:21:56 -0600
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.5.31 to
    1.6.0.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.31...v1.6.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump kotlin-stdlib from 1.5.31 to 1.6.0 (#684)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Dec 2021 09:20:40 -0600
    
    
    Bumps [kotlin-stdlib](https://github.com/JetBrains/kotlin) from 1.5.31 to
    1.6.0.
    - [Release notes](https://github.com/JetBrains/kotlin/releases)
    - [Changelog](https://github.com/JetBrains/kotlin/blob/master/ChangeLog.md)
    - [Commits](https://github.com/JetBrains/kotlin/compare/v1.5.31...v1.6.0)
    
    ---
    updated-dependencies:
    - dependency-name: org.jetbrains.kotlin:kotlin-stdlib
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Bump jackson-bom from 2.12.5 to 2.13.0 (#685)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Wed, 1 Dec 2021 09:19:39 -0600
    
    
    Bumps [jackson-bom](https://github.com/FasterXML/jackson-bom) from 2.12.5 to
    2.13.0.
    - [Release notes](https://github.com/FasterXML/jackson-bom/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-bom/compare/jackson-bom-2.12.5...jackson-bom-2.13.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson:jackson-bom
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Corrected the starting version for the Processor renaming in the configuration.md file. (#668)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Nov 2021 17:06:00 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Set the next version to Data Prepper 1.3.0. (#670)__

    [David Venable](mailto:dlv@amazon.com) - Tue, 30 Nov 2021 15:32:59 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __reverting pipeline yamls use of processor to prepper. (#667)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 30 Nov 2021 14:17:06 -0600
    
    
    DP v1.2 is scheduled for release in a week. However a previous change migrated
    all example pipelines
    to use processors. Reverting the change to keep our
    existing examples working. This commit can be reverted
    once DP v1.2 is
    released.
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Rename prepper to processor (#655)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Tue, 30 Nov 2021 11:51:44 -0600
    
    
    Removed references to Preppers in data-prepper-core and docs except where
    required for plugin compatibility.
    Added testing for support of both Prepper
    and Processor plugins.
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __remove items from pull request template which are covered through git… (#660)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Tue, 30 Nov 2021 11:42:47 -0600
    
    
    * remove items from pull request template which are covered through github
    actions (#660)
     Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Updated peer forwarder test timeout (#654)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 29 Nov 2021 16:48:40 -0600
    
    
    * Updated peer forwarder test timeout
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Udated test timeout to 30 seconds
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __correcting directional arrows on overview diagram (#659)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Wed, 24 Nov 2021 15:41:06 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;

* __Maintenance: migrate stdin and random source to event model (#625)__

    [Qi Chen](mailto:19492223+chenqi0805@users.noreply.github.com) - Tue, 23 Nov 2021 12:31:44 -0600
    
    
    * Added md for Log Ingestion Demo Guide
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * More fixes
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * Refactored file source, added type and format configuration options for json
    and plaintext
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * Address PR comments
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * Changed format JsonProperty to String, removed file source from
    DataPrepperTest
     Signed-off-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;
    
    * MAINT: migrate stdin and random source to use event model
     Signed-off-by: qchea &lt;qchea@amazon.com&gt;
    
    * MAINT: update readme
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: minor cleanup
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: remove write_timeout
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: revert some changes
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
    
    * MAINT: remove blockign buffer depedency
     Signed-off-by: Chen &lt;19492223+chenqi0805@users.noreply.github.com&gt;
     Co-authored-by: Taylor Gray &lt;33740195+graytaylor0@users.noreply.github.com&gt;

* __Update maintainers (#651)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Tue, 23 Nov 2021 11:40:47 -0600
    
    
    * Update Maintiners.md
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;
    
    * Make alphabetical
     Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Bump jackson-core in /data-prepper-plugins/opensearch (#529)__

    [dependabot[bot]](mailto:49699333+dependabot[bot]@users.noreply.github.com) - Mon, 22 Nov 2021 16:46:03 -0600
    
    
    Bumps [jackson-core](https://github.com/FasterXML/jackson-core) from 2.12.5 to
    2.13.0.
    - [Release notes](https://github.com/FasterXML/jackson-core/releases)
    -
    [Commits](https://github.com/FasterXML/jackson-core/compare/jackson-core-2.12.5...jackson-core-2.13.0)
    
    
    ---
    updated-dependencies:
    - dependency-name: com.fasterxml.jackson.core:jackson-core
     dependency-type: direct:production
     update-type: version-update:semver-minor
    ...
     Signed-off-by: dependabot[bot] &lt;support@github.com&gt;
     Co-authored-by: dependabot[bot]
    &lt;49699333+dependabot[bot]@users.noreply.github.com&gt;

* __Simple guide to run Data Prepper with Logstash config (#616)__

    [Shivani Shukla](mailto:67481911+sshivanii@users.noreply.github.com) - Mon, 22 Nov 2021 16:45:36 -0600
    
    
    Added a simple Guide to run Data Prepper with Logstash config
    Signed-off-by:
    Shivani Shukla &lt;sshkamz@amazon.com&gt;

* __Some minor clean up for the Logstash configuration test data provider. (#636)__

    [David Venable](mailto:dlv@amazon.com) - Mon, 22 Nov 2021 15:14:10 -0600
    
    
    Signed-off-by: David Venable &lt;dlv@amazon.com&gt;

* __Moving processor from generic to Record&lt;Event&gt; (#620)__

    [David Powers](mailto:37314042+dapowers87@users.noreply.github.com) - Mon, 22 Nov 2021 13:00:18 -0600
    
    
    Signed-off-by: David Powers &lt;ddpowers@amazon.com&gt;

* __Rename prepper to processor (#594)__

    [Steven Bayer](mailto:sbayer55@gmail.com) - Mon, 22 Nov 2021 10:58:46 -0600
    
    
    * deprecate Prepper class
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Add testing for prepper or processor config
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Update documentation to use processor in place of prepper
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Add overload constructor for pipeline model and config
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * log warning if prepper config is used
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * move deprecated prepper warning to always log if prepper is defined
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * update config files to use processor in place of prepper
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * Updated documentation to use processor configurations
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
    
    * update javadocs to specify version functionality was introduced
     Signed-off-by: Steven Bayer &lt;smbayer@amazon.com&gt;
     Co-authored-by: Steven Bayer &lt;smbayer@amazon.com&gt;

* __codifying peer forwarder local client (#626)__

    [Christopher Manning](mailto:cmanning09@users.noreply.github.com) - Thu, 18 Nov 2021 16:53:45 -0600
    
    
    Signed-off-by: Christopher Manning &lt;cmanning09@users.noreply.github.com&gt;


