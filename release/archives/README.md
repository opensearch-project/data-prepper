## Archive distribution of Data Prepper

**Supported Platform**

* Linux - x64
* Linux - arm64

### General Instructions

#### Building Archives

To build all archives, check out the corresponding branch for the version and follow below steps

1. Building an archive depends on the platform on which it will be executed/run
2. For building archives for all supported platforms, from root project run  `./gradlew :release:archives:buildArchives`
3. Successful build will generate archives in `release/archives/<platform>/build/distributions/` directory
4. Generated archives includes a script file which can be used to execute the data prepper using
 
```
tar -xzf opensearch-data-prepper-jdk-v<VERSION>-<PLATFORM>-<ARCHITECTURE>.tar.gz
./opensearch-data-prepper-jdk-v<VERSION>-<PLATFORM>-<ARCHITECTURE>/bin/data-prepper <CONFIG FILE LOCATION>
```

#### For platform specific archive

1. From root project, run `./gradlew :release:archives:<platform>:<platform>Tar`
2. Successful build generates archives in `release/archives/<platform>/build/distributions/`

##### Example for linux

For linux, the above steps will be
 ```
// from root project
./gradlew :release:archives:linux:linuxTar -Prelease

cd release/archives/linux/build/distributions/

tar -xzf opensearch-data-prepper-jdk-<VERSION>-linux-<ARCHITECTURE>.tar.gz
//e.g tar -xzf opensearch-data-prepper-jdk-2.14.0-linux-x64.tar.gz

./opensearch-data-prepper-jdk-<VERSION>-linux-<ARCHITECTURE>/bin/data-prepper <CONFIG FILE LOCATION>

```

#### Uploading the archive to S3

The build also includes mechanisms to upload the built archive to S3 for distribution. Uploading the archive to S3 
assumes the executor has appropriate credentials in his/her `.awsCredentials` directory. *refer [aws docs](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html#credentials-file-format) for more information*
Also each platform specific project has a similar mechanism to upload platform specific archives to S3.

* From root project, run 

```
./gradlew :release:archives:uploadArchives -Pregion=us-east-1 -Pbucket=data-prepper-my-bucket -Pprofile=default

or

//below command will use default values for region, bucket and profile
./gradlew :release:archives:uploadArchives 
```

or

* From current project, run

```
./gradle uploadArchives -Pregion=us-east-1 -Pbucket=data-prepper-my-bucket -Pprofile=default

or

//below command will use default values for region, bucket and profile
./gradlew uploadArchives 

```
