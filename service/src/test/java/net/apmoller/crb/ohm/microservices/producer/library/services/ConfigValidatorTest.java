package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaHeaderValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { ConfigValidator.class })
@ActiveProfiles({ "test" })
public class ConfigValidatorTest<T> {

    @Autowired
    private ConfigValidator<T> validator;

    @MockBean
    private ApplicationContext context;

    @Test
    void testInvalidTopic() {
        String producerTopic = "";
        String message = "test";
        assertThrows(TopicNameValidationException.class, () -> validator.validateInputs(producerTopic, (T) message));
    }

    @Test
    void testInvalidTopicPlaceholder() {
        String producerTopic = "${";
        String message = "test";
        assertThrows(TopicNameValidationException.class, () -> validator.validateInputs(producerTopic, (T) message));
    }

    @Test
    void testInvalidServerPlaceholder() {
        String producerTopic = "test";
        String message = "test";
        assertThrows(KafkaServerNotFoundException.class, () -> validator.validateInputs(producerTopic, (T) message));
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenInputMapEmpty() {
        Map<String, String> topicMap = new HashMap<>();
        String message = "test";
        assertThrows(TopicNameValidationException.class,
                () -> validator.validateInputsForMultipleProducerFlow(topicMap, (T) message));
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenTargetTopicNotPresent() {
        Map<String, String> topicMap = new HashMap<>();
        String message = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        Throwable exception = assertThrows(TopicNameValidationException.class,
                () -> validator.validateInputsForMultipleProducerFlow(topicMap, (T) message));
        assertEquals(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG, exception.getMessage());
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenRetryTopicNotPresent() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        var retryTopicPresent = validator.retryTopicPresent(topicMap);
        assertTrue(retryTopicPresent);
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenDltNotPresent() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        var dltTopicPresent = validator.dltTopicPresent(topicMap);
        assertTrue(dltTopicPresent);
    }

    @Test
    void testInvalidServerPlaceholderForMultipleProducer() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String message = "test";
        assertThrows(KafkaServerNotFoundException.class,
                () -> validator.validateInputsForMultipleProducerFlow(topicMap, (T) message));
    }

    @Test
    void testRetryTopicIsPresent() {
        String retryTopic = "retryTopic";
        assertEquals(Boolean.TRUE, validator.retryTopicIsPresent(retryTopic));
    }

    @Test
    void testClaimsCheckTopicIsPresent() {
        String retryTopic = "claimsCheck";
        assertEquals(Boolean.TRUE, validator.claimsCheckTopicIsPresent(retryTopic));
    }
    @Test
    void testClaimsCheckTopicIsNotPresent() {
        assertEquals(Boolean.FALSE, validator.claimsCheckTopicIsPresent(""));
    }

    @Test
    void testRetryTopicIsNotPresent() {
        assertEquals(Boolean.FALSE, validator.retryTopicIsPresent(""));
    }

    @Test
    void testDltTopicIsPresent() {
        String dltTopic = "dltTopic";
        assertEquals(Boolean.TRUE, validator.dltTopicIsPresent(dltTopic));
    }

    @Test
    void testDltTopicIsNotPresent() {
        assertEquals(Boolean.FALSE, validator.dltTopicIsPresent(null));
    }

    @Test
    void testEmptyPayload() {
        String producerTopic = "test";
        assertThrows(PayloadValidationException.class, () -> validator.validateInputs(producerTopic, null));
    }

    @Test
    void testInputValidationExceptionWhenTopicNameValidationException() {
        assertTrue(validator.isInputValidationException(
                new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG)));
    }

    @Test
    void testInputValidationExceptionWhenKafkaBootstrapServerException() {
        assertTrue(validator.isInputValidationException(
                new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG)));
    }

    @Test
    void testInputValidationExceptionWhenPayloadValidationException() {
        assertTrue(validator
                .isInputValidationException(new PayloadValidationException(ConfigConstants.INVALID_PAYLOAD_ERROR_MSG)));
    }

    @Test
    void testInputValidationExceptionWhenKafkaHeaderValidationException() {
        assertTrue(validator.isInputValidationException(
                new KafkaHeaderValidationException(ConfigConstants.INVALID_KAFKA_HEADER_VALUE_ERROR_MSG)));
    }

    @Test
    void testInputValidationExceptionWhenNegativeScenario() {
        assertFalse(validator.isInputValidationException(new TimeoutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicForMultipleProducerWhenTimeOutException() {
        Map<String, String> topics = new HashMap<>();
        topics.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        assertTrue(validator.sendToRetryTopic(topics, new TimeoutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicForMultipleProducerWhenTransactionTimeOutException() {
        Map<String, String> topics = new HashMap<>();
        topics.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        assertTrue(validator.sendToRetryTopic(topics, new TransactionTimedOutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicForMultipleProducerWhenTopicAuthorizationException() {
        Map<String, String> topics = new HashMap<>();
        topics.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        assertTrue(validator.sendToRetryTopic(topics, new KafkaException(new TopicAuthorizationException("test"))));
    }

    @Test
    void testConditionForPostingToRetryTopicForMultipleProducerWhenKafkaException() {
        Map<String, String> topics = new HashMap<>();
        topics.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        assertFalse(validator.sendToRetryTopic(topics, new KafkaException()));
    }

    @Test
    void testConditionForPostingToRetryTopicForMultipleProducerWhenTopicNotPresentInInput() {
        Map<String, String> topics = new HashMap<>();
        topics.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertFalse(validator.sendToRetryTopic(topics, new TimeoutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicWhenTimeOutException() {
        assertTrue(validator.sendToRetryTopic("retry", new TimeoutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicWhenTransactionTimeOutException() {
        assertTrue(validator.sendToRetryTopic("retry", new TransactionTimedOutException("test")));
    }

    @Test
    void testConditionForPostingToRetryTopicWhenTopicAuthorizationException() {
        assertTrue(validator.sendToRetryTopic("retry", new KafkaException(new TopicAuthorizationException("test"))));
    }

    @Test
    void testConditionForPostingToRetryTopicWhenKafkaException() {
        assertFalse(validator.sendToRetryTopic("retry", new KafkaException()));
    }

    @Test
    void testConditionForPostingToRetryTopicWhenTopicNameNull() {
        assertFalse(validator.sendToRetryTopic("", new TimeoutException()));
    }

    @Test
    void testDltValidationWhenNullValuePassed() {
        assertFalse(validator.dltTopicIsPresent(null));
    }

    @Test
    void testDltValidationWhenEmptyValuePassed() {
        assertFalse(validator.dltTopicIsPresent(""));
    }

    @Test
    void testDltValidationWhenConfigNotAdded() {
        assertFalse(validator.dltTopicIsPresent(ConfigConstants.DLT));
    }

    @Test
    void testRetryTopicValidationWhenNullValuePassed() {
        assertFalse(validator.retryTopicIsPresent(null));
    }

    @Test
    void testRetryTopicValidationWhenEmptyValuePassed() {
        assertFalse(validator.retryTopicIsPresent(""));
    }

    @Test
    void testRetryTopicValidationWhenConfigNotAdded() {
        assertFalse(validator.dltTopicIsPresent(ConfigConstants.RETRY_TOPIC));
    }

    @Test
    void testBootstrapServerValidationWhenNullValuePassed() {
        assertThrows(KafkaServerNotFoundException.class, () -> validator.bootstrapServerValidation(null));
    }

    @Test
    void testBootstrapServerValidationWhenEmptyValuePassed() {
        assertThrows(KafkaServerNotFoundException.class, () -> validator.bootstrapServerValidation(""));
    }

}
