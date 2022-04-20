FROM openjdk:11-jdk
VOLUME /tmp
WORKDIR /app
COPY service/target/kafkaproducer-service.jar /app/
RUN wget -O dd-java-agent.jar 'https://repository.sonatype.org/service/local/artifact/maven/redirect?r=central-proxy&g=com.datadoghq&a=dd-java-agent&v=LATEST'
ENTRYPOINT ["java", "-javaagent:dd-java-agent.jar", "-Ddd.service.name=kafkaproducer-service","-Ddd.logs.injection=true", "-jar", "kafkaproducer-service.jar"]


