package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;
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

        notificationTopicValidation(producerTopic);
        bootstrapServerValidation(bootstrapServer);
        retryTopicValidation(retryTopic);
        dltTopicValidation(dltTopic);
    }

    /**
     * Method checks if Main topic is Valid.
     *
     * @param producerTopic
     */
    private void notificationTopicValidation(String producerTopic) {
        if ((producerTopic.startsWith("${"))) {
            throw new InvalidTopicException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_PLACEHOLDER);
        } else if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new InvalidTopicException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        }
    }

    /**
     * Method checks if retry topic is Valid.
     *
     * @param retryTopic
     */
    private void retryTopicValidation(String retryTopic) {
        if ((retryTopic.startsWith("${"))) {
            throw new InvalidTopicException(ConfigConstants.INVALID_RETRY_TOPIC_PLACEHOLDER);
        } else if (Objects.isNull(retryTopic) || retryTopic.isEmpty()) {
            throw new InvalidTopicException(ConfigConstants.INVALID_RETRY_TOPIC_ERROR_MSG);
        }
    }

    /**
     * Method checks if dead letter topic is valid.
     *
     * @param dltTopic
     */
    private void dltTopicValidation(String dltTopic) {
        if (dltTopic.startsWith("${")) {
            throw new InvalidTopicException(ConfigConstants.INVALID_DLT_TOPIC_PLACEHOLDER);
        } else if ((Objects.isNull(dltTopic) || dltTopic.isEmpty())) {
            throw new InvalidTopicException(ConfigConstants.INVALID_DLT_ERROR_MSG);
        }
    }

    /**
     * Method checks if bootstrapServer is valid.
     *
     * @param bootstrapServer
     */
    private void bootstrapServerValidation(String bootstrapServer) {
        if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_PLACEHOLDER);
        } else if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        }
    }

    /**
     * Method to validate topic and bootstrap server before posting message to kafka topic.
     * @param topics
     */
    public void validateInputsForMultipleProducerFlow(Map<String, String> topics) {

        log.info("Topics map passed in input: {}", topics);
        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        log.info("bootstrapServer from application context: {}", bootstrapServer);
        if (Objects.isNull(topics) || topics.isEmpty()) {
            throw new InvalidTopicException(ConfigConstants.INVALID_TOPIC_MAP_ERROR_MSG);
        } else {
            if (notificationTopicNotPresent(topics))
                throw new InvalidTopicException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
            if (retryTopicNotPresent(topics))
                throw new InvalidTopicException(ConfigConstants.INVALID_RETRY_TOPIC_ERROR_MSG);
            if (dltNotPresent(topics))
                throw new InvalidTopicException(ConfigConstants.INVALID_DLT_ERROR_MSG);
        }

        if (bootstrapServer.isEmpty() || bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        }
    }

    /**
     * Method checks if input map contains target topic name
     * @param topics
     */
    private boolean notificationTopicNotPresent(Map<String, String> topics) {
        return !topics.containsKey(ConfigConstants.NOTIFICATION_TOPIC_KEY)
                || (topics.containsKey(ConfigConstants.NOTIFICATION_TOPIC_KEY) && Objects.isNull(topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY)));
    }

    /**
     * Method checks if input map contains retry topic name
     * @param topics
     */
    private boolean retryTopicNotPresent(Map<String, String> topics) {
        return !topics.containsKey(ConfigConstants.RETRY_TOPIC_KEY)
                || (topics.containsKey(ConfigConstants.RETRY_TOPIC_KEY) && Objects.isNull(topics.get(ConfigConstants.RETRY_TOPIC_KEY)));
    }

    /**
     * Method checks if input map contains dead letter topic name
     * @param topics
     */
    private boolean dltNotPresent(Map<String, String> topics) {
        return !topics.containsKey(ConfigConstants.DEAD_LETTER_TOPIC_KEY)
                || (topics.containsKey(ConfigConstants.DEAD_LETTER_TOPIC_KEY) && Objects.isNull(topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY)));

    }
}
