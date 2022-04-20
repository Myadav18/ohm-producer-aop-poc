**ohm-microservices**

**Description**

This is a Yeoman generator to scaffold out a Spring-Boot service for a Maersk project.

Yeoman is a Node.js tool, and so NPM (Node Package Manager) will be required.

**Installation**

1)Install NPM tooling on your machine, ref: https://nodejs.org/en/download/ (If you have an older version, update it with npm install -g npm@latest)

2)Install Yeoman: npm install yo -g

3)Clone the Maersk Spring Template repository and build it

git clone https://github.com/Maersk-Global/template-generator-ohm-microservices.git

cd template-generator-ohm-microservices

npm install

npm link

after above step type YO on CMD and see it displays ohm-microservices

4)Run Yeoman on this Maersk Spring Template and follow the onscreen instructions:

cd ..

mkdir myProjectName

cd myProjectName

yo ohm-microservices

## Yeoman Options
The following are options available when running the Yeoman tool.

#### Service Name
Description: The name of your project will be.

Default: myapp

#### Rest Option
Description: Do you want a sample REST endpoint? If yes,  it will create a hello world sample endpoint with unit tests, it will add dependencies in the pom.

Default: N

#### Cassandra Option
Description: Do you want Cassandra database options? If yes, it will add a sample domain object and cassandra repository, it will add cassandra connection proprties and dependencies in the pom.

Default: N

#### Kafka Producer Option
Description: Do you want a Kafka Producer µS? If yes, it will add the related classes and dependencies in the pom, it will add `-producer` at the end of your service name.

Default: N

Note : Please do not select rest option when you select Kafka Producer Option

#### Kafka Consumer Option
Description: Do you want a Kafka Consumer µS? If yes, it will add the related classes and dependencies in the pom, it will add `-consumer` at the end of your service name.

Default: N

Note : Please do not select rest option when you select Kafka Consumer Option

#### BuildTool

Description: Which build tool do you want to use

Default: maven

Note : Gradle build tool support is not provided for now

#### which modules you need to create

Description:  Select modules (ex: service,spec,integrationtest,loadtest)

Default: parent


**Note**:

confluence link for same : https://confluence.maerskdev.net/display/OHM/Scaffolding+microservices+project

If you have selected Kafka Templates, you can refer : https://confluence.maerskdev.net/display/OHM/Kafka+Templates


#Service Project Structure

## spec
Basic swagger spec definition for the project path : `spec/src/main/resource/swagger.yaml`.

## integration test

To execute integration tests, Run RunCukesIT, path : integrationtest/src/test/java/net/apmoller/crb/microservices/multi/module/demoProject/RunCukesIT.java

Make sure docker desktop is open

## service
Spring boot 1.x based services by default. A common set of utilities is also included, see: http://git.devops.apmoller.net/projects/MLIT_PORTAL/repos/ao-spring-boot-components/browse/

##Note : Before starting server or making any push, please perform mvn clean install as a mandatory step.

To start server locally for manual testing/debugging: `mvn spring-boot:run`


## CI/CD

For CI/CD we are using azure Build and release pipelines please refer below video for all below steps

https://my.maerskgroup.com/:v:/g/personal/aliviya_saha_maersk_com/EfTmWiPBx8NJtOEZMFjvo0UBTl9FiJ4AXYk8YI9QfUtpmg

if you are not able to access plz in touch with Aliviya
