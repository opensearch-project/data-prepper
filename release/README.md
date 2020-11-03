## Description

This repository contains the docker image for odfe-pipes.

## Running a docker build

To build an image, check out the corresponding branch for the version and follow below steps

1. From root project, run `./gradlew clean :release:docker:docker -Prelease`
2. The image will be built and will be available for running using docker

### Running built docker image

The built image will be available locally and can be used as any other docker image. The root project has some usage examples for reference included in `examples` directory. Below is an example command which can also be used to run the built image.

```
docker run --name situp-test --expose 21890 --read-only -v /home/ec2-user/situp-config.yml:/usr/share/situp/situp.yml situp/situp:0.1-beta

```

##  Tar build

**Supported Platform**

* Linux
* Mac OS

### Running a tar build

To build a tar, check out the corresponding branch for the version and follow below steps

1. Building a tar depends on the platform on which it will be executed/run
2. From the root project, run  `./gradlew clean :release:<platform>:<platform>Tar -Prelease`
3. Successful build should generate two tars in `release/<platform>/build/distributions/` directory
4. Generated tars includes a script file which can be used to execute the situp using 
```
tar -xzf situp-<platform>-jdk-<VERSION>.tar.gz
./situp-<platform>-jdk-<VERSION>/situp-tar-install.sh <CONFIG FILE LOCATION>
```

### Example
* For linux platform, above steps will be as below
 ```
./gradlew clean :release:linux:linuxTar -Prelease

cd release/linux/build/distributions/

tar -xzf situp-linux_x86_64-jdk-<VERSION>.tar.gz

./situp-linux_x86_64-jdk-<VERSION>/situp-tar-install.sh <CONFIG FILE LOCATION>

```

* For mac OS, above steps will be as below
 ```
./gradlew clean :release:macOS:macOSTar -Prelease

cd release/macOS/build/distributions/

tar -xzf situp-macOS_x64-jdk-<VERSION>.tar.gz

./situp-macOS_x64-jdk-<VERSION>/situp-tar-install.sh <CONFIG FILE LOCATION>

```
   

