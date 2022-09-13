## Docker Build for Data Prepper

### Running a docker build

To build an image, check out the corresponding branch for the version and follow below steps

* From root project, run 

```
./gradlew clean :release:docker:docker -Prelease
```
or 

* if running from the current project 
    
``` 
gradle docker -Prelease
```
    
* The image will be built and will be available for running using docker

### Running built docker image

The built image will be available locally and can be used as any other docker image. 
The root project has some usage examples for reference included in `examples` directory. 
Below is an example command which can also be used to run the built image.

```
docker run \
 --name data-prepper-test \
 -p 21890:21890 -p 4900:4900 \
 -v ${PWD}/examples/config/example-pipelines.yaml:/usr/share/data-prepper/pipelines/pipelines.yaml \
 -v ${PWD}/examples/config/example-data-prepper-config.yaml:/usr/share/data-prepper/config/data-prepper-config.yaml \
 opensearch-data-prepper:2.0.0-SNAPSHOT
```
