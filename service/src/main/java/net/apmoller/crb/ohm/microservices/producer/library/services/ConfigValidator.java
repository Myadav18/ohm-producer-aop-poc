package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class ConfigValidator {

    public void validateInputs(String producerTopic, String bootstrapServer) {

        if (producerTopic.startsWith("${")) {
            throw new InvalidTopicException("Topic Placeholder is not valid");
        }
        if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new InvalidTopicException("Topic name is not valid , it can't be null or Empty");
        }

        if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException("Bootstrap details cannot be empty, so unable to be connect");
        }

        if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException("Placeholder for Bootstrap Server is not Correct");
        }

    }
}
