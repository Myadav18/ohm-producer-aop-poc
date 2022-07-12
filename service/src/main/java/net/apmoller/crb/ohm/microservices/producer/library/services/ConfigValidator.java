package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaHeaderValidationException;
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
     * @param producerTopic - target topic name
     * @param message - payload
     */
    public void validateInputs(String producerTopic, T message) {
        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        payloadValidation(message);
        targetTopicValidation(producerTopic);
        bootstrapServerValidation(bootstrapServer);
    }

    /**
     * Method checks if payload is not null.
     *
     * @param message - payload
     */
    private void payloadValidation(T message) {
        if (Objects.isNull(message)) {
            throw new PayloadValidationException(ConfigConstants.INVALID_PAYLOAD_ERROR_MSG);
        }
    }

    /**
     * Method checks if Main topic is Valid.
     *
     * @param producerTopic - target topic name
     */
    private void targetTopicValidation(String producerTopic) {
        if (Objects.isNull(producerTopic) || producerTopic.isEmpty()) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        } else if ((producerTopic.startsWith("${"))) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_PLACEHOLDER);
        }
    }

    public boolean claimsCheckTopicNotPresent(String claimsCheckTopic) {
        return Objects.isNull(claimsCheckTopic) || claimsCheckTopic.isEmpty() || claimsCheckTopic.startsWith("${");
    }

    /**
     * Method checks if dead letter topic is valid.
     *
     * @param dltTopic - dead letter topic name from config
     */
    public boolean dltTopicIsPresent(String dltTopic) {
        return ((Objects.nonNull(dltTopic)) && !(dltTopic.isEmpty()) && !(dltTopic.startsWith("${")));
    }

    /**
     * Method checks if bootstrapServer is valid.
     *
     * @param bootstrapServer - kafka bootstrap server value from config
     */
    public void bootstrapServerValidation(String bootstrapServer) {
        if (Objects.isNull(bootstrapServer) || bootstrapServer.isEmpty()) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        } else if (bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_PLACEHOLDER);
        }
    }

    /**
     * Method to validate topic and bootstrap server before posting message to kafka topic.
     *
     * @param topics - Topics name map from input
     * @param message - payload
     */
    public void validateInputsForMultipleProducerFlow(Map<String, String> topics, T message) {

        log.info("Topics map passed in input: {}", topics);
        var bootstrapServer = context.getEnvironment().resolvePlaceholders(ConfigConstants.BOOTSTRAP_SERVER);
        log.info("bootstrapServer from application context: {}", bootstrapServer);
        payloadValidation(message);
        if (Objects.isNull(topics) || topics.isEmpty()) {
            throw new TopicNameValidationException(ConfigConstants.INVALID_TOPIC_MAP_ERROR_MSG);
        } else {
            if (targetTopicNotPresent(topics))
                throw new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        }

        if (bootstrapServer.isEmpty() || bootstrapServer.startsWith("${")) {
            throw new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        }
    }

    /**
     * Method checks if input map contains target topic name
     *
     * @param topics - Topics name map from input
     */
    private boolean targetTopicNotPresent(Map<String, String> topics) {
        return !topics.containsKey(ConfigConstants.NOTIFICATION_TOPIC_KEY)
                || (topics.containsKey(ConfigConstants.NOTIFICATION_TOPIC_KEY)
                        && ((Objects.isNull(topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY)))
                                || ((topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY)).isEmpty())));
    }

    /**
     * Method checks if input map contains dead letter topic name
     * @param topics - Topics name map from input
     */
    public boolean dltTopicPresent(Map<String, String> topics) {
        return (topics.containsKey(ConfigConstants.DEAD_LETTER_TOPIC_KEY)
                && (Objects.nonNull(topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY))))
                && !((topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY)).isEmpty());

    }

    public boolean claimsCheckTopicNotPresent(Map<String, String> topics) {
        return !topics.containsKey(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY)
                || (topics.containsKey(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY)
                        && (Objects.isNull(topics.get(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY))
                                || (topics.get(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY)).isEmpty()
                                || topics.get(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY).startsWith("${")));
    }

    public boolean claimsCheckDltPresent(Map<String, String> topics) {
        return topics.containsKey(ConfigConstants.CLAIMS_CHECK_DLT_KEY)
                && (Objects.nonNull(topics.get(ConfigConstants.CLAIMS_CHECK_DLT_KEY))
                        && (!(topics.get(ConfigConstants.CLAIMS_CHECK_DLT_KEY)).isEmpty()));
    }

    /**
     * Method validates the type of Runtime exception that occurred on target topic
     * @param ex - Runtime exception
     */
    public boolean isInputValidationException(RuntimeException ex) {
        return ((ex instanceof KafkaServerNotFoundException) || (ex instanceof TopicNameValidationException)
                || (ex instanceof PayloadValidationException) || (ex instanceof KafkaHeaderValidationException));
    }
}
