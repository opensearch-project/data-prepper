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
docker run --name data-prepper-test --expose 21890 --read-only -v /home/ec2-user/data-prepper-config.yml:/usr/share/data-prepper/data-prepper.yml data-prepper/data-prepper:0.1.0

```