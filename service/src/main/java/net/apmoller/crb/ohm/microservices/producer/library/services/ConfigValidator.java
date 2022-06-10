package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class ConfigValidator<T> {

    @Autowired
    private ApplicationContext context;

    /**
     * Method checks the validation before posting message to kafka topic.
     *
     * @param producerTopic
     */
    public void validateInputs(String producerTopic, T message) {

        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        // var retryTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.RETRY_TOPIC);
        // var dltTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.DLT);

        payloadValidation(message);
        notificationTopicValidation(producerTopic);
        bootstrapServerValidation(bootstrapServer);
        // retryTopicValidation(retryTopic);
        // dltTopicValidation(dltTopic);
    }

    /**
     * Method checks if payload is not null.
     *
     * @param message
     */
    private void payloadValidation(T message) {
        if (Objects.isNull(message)) {
            throw new PayloadValidationException(ConfigConstants.INVALID_PAYLOAD_ERROR_MSG);
        }
    }

    /**
     * Method checks if Main topic is Valid.
     *
     * @param producerTopic
     */
    private void notificationTopicValidation(String producerTopic) {
        if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        } else if ((producerTopic.startsWith("${"))) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_PLACEHOLDER);
        }
    }

    /**
     * Method checks if retry topic is Valid.
     *
     * @param retryTopic
     */
    public boolean retryTopicIsPresent(String retryTopic) {
        return ((Objects.nonNull(retryTopic)) && !(retryTopic.isEmpty()) && !((retryTopic.startsWith("${"))));
    }

    /**
     * Method checks if dead letter topic is valid.
     *
     * @param dltTopic
     */
    public boolean dltTopicIsPresent(String dltTopic) {
        return (!(Objects.nonNull(dltTopic)) && !(dltTopic.isEmpty()) && !((dltTopic.startsWith("${"))));
    }

    /**
     * Method checks if bootstrapServer is valid.
     *
     * @param bootstrapServer
     */
    private void bootstrapServerValidation(String bootstrapServer) {
        if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        } else if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_PLACEHOLDER);
        }
    }

    /**
     * Method to validate topic and bootstrap server before posting message to kafka topic.
     * @param topics
     */
    public void validateInputsForMultipleProducerFlow(Map<String, String> topics, T message) {

        log.info("Topics map passed in input: {}", topics);
        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        log.info("bootstrapServer from application context: {}", bootstrapServer);
        payloadValidation(message);
        if (Objects.isNull(topics) || topics.isEmpty()) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_TOPIC_MAP_ERROR_MSG);
        } else {
            if (notificationTopicNotPresent(topics))
                throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
            // if (retryTopicNotPresent(topics))
            // throw new TopicNameValidationException(ConfigConstants.INVALID_RETRY_TOPIC_ERROR_MSG);
            // if (dltNotPresent(topics))
            // throw new TopicNameValidationException(ConfigConstants.INVALID_DLT_ERROR_MSG);
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
                || (topics.containsKey(ConfigConstants.NOTIFICATION_TOPIC_KEY)
                        && Objects.isNull(topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY)));
    }

    /**
     * Method checks if input map contains retry topic name
     * @param topics
     */
    public boolean retryTopicPresent(Map<String, String> topics) {
        return topics.containsKey(ConfigConstants.RETRY_TOPIC_KEY)
                && (Objects.nonNull(topics.get(ConfigConstants.RETRY_TOPIC_KEY)));
    }

    /**
     * Method checks if input map contains dead letter topic name
     * @param topics
     */
    public boolean dltTopicPresent(Map<String, String> topics) {
        return  (topics.containsKey(ConfigConstants.DEAD_LETTER_TOPIC_KEY)
                        && (Objects.nonNull(topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY))));

    }
}
