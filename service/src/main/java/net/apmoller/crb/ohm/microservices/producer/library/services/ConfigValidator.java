package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
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

    @Autowired
    private ApplicationContext context;

    /**
     * Method checks the validation before posting message to kafka topic.
     * 
     * @param producerTopic
     */
    public void validateInputs(String producerTopic) {

        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        var retryTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.RETRY_TOPIC);
        var dltTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.DLT);

        if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new InvalidTopicException("Topic name is not valid , it can't be null or Empty");
        } else if (Objects.isNull(retryTopic) || retryTopic.isEmpty()) {
            throw new InvalidTopicException("Retry Topic name is not valid , it can't be null or Empty");
        } else if (Objects.isNull(dltTopic) || dltTopic.isEmpty()) {
            throw new InvalidTopicException("Dlt Topic name is not valid , it can't be null or Empty");
        }
        if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException("Bootstrap details cannot be empty, so unable to be connect");
        }

        if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException("Placeholder for Bootstrap Server is not Correct");
        }

        if ((producerTopic.startsWith("${")) || (retryTopic.startsWith("${")) || (dltTopic.startsWith("${"))) {
            throw new InvalidTopicException("Placeholder for Main_topic/Retry_topic/ Dlt_Topic is not valid");
        }

    }
}
