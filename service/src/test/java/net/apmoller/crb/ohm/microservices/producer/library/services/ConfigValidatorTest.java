package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { ConfigValidator.class })
@ActiveProfiles({ "test" })
public class ConfigValidatorTest {

    @Autowired
    private ConfigValidator validator;

    @MockBean
    private ApplicationContext context;

    @Test
    void testInvalidTopic() {
        String producerTopic = "";
        assertThrows(InvalidTopicException.class, () -> validator.validateInputs(producerTopic));
    }

    @Test
    void testInvalidTopicPlaceholder() {
        String producerTopic = "${";
        assertThrows(InvalidTopicException.class, () -> validator.validateInputs(producerTopic));
    }

    @Test
    void testInvalidServerPlaceholder() {
        String producerTopic = "test";
        assertThrows(KafkaServerNotFoundException.class, () -> validator.validateInputs(producerTopic));
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenInputMapEmpty() {
        Map<String,String> topicMap = new HashMap<>();
        assertThrows(InvalidTopicException.class, () -> validator.validateInputsForMultipleProducerFlow(topicMap));
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenTargetTopicNotPresent() {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        Throwable exception = assertThrows(InvalidTopicException.class, () -> validator.validateInputsForMultipleProducerFlow(topicMap));
        assertEquals(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG, exception.getMessage());
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenRetryTopicNotPresent() {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        Throwable exception = assertThrows(InvalidTopicException.class, () -> validator.validateInputsForMultipleProducerFlow(topicMap));
        assertEquals(ConfigConstants.INVALID_RETRY_TOPIC_ERROR_MSG, exception.getMessage());
    }

    @Test
    void testInvalidTopicForMultipleProducerWhenDltNotPresent() {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        Throwable exception = assertThrows(InvalidTopicException.class, () -> validator.validateInputsForMultipleProducerFlow(topicMap));
        assertEquals(ConfigConstants.INVALID_DLT_ERROR_MSG, exception.getMessage());
    }

    @Test
    void testInvalidServerPlaceholderForMultipleProducer() {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertThrows(KafkaServerNotFoundException.class, () -> validator.validateInputsForMultipleProducerFlow(topicMap));
    }

}
