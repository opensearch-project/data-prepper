## Description

This repository contains the docker image for odfe-pipes.

## Running a docker build

To build an image, check out the corresponding branch for the version and run `./gradlew clean :release:docker:docker -Prelease`

## Running a tar build

To build a tar, check out the corresponding branch for the version and run `./gradlew clean :release:linux:linuxDistTar -Prelease`

