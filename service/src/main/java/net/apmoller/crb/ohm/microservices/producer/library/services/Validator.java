package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class Validator {

    public void checkValidation(String producerTopic, String bootstrapServer) {
        if (producerTopic.startsWith("${")) {
            throw new InvalidTopicException("Topic Placeholder is not valid");
        }
        if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new InvalidTopicException("Topic name is not valid , it can't be null or Empty");
        }
        if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException("Placeholder for Bootstrap Server is not Correct");
        }
        if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException(
                    "Bootstrap Server is not able to Connected, it can't be Empty or null");
        }
    }
}
