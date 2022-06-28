package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@EnableRetry
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { KafkaProducerServiceImpl.class })
@ActiveProfiles({ "test" })
public class KafkaProducerServiceTest<T> {

    @MockBean
    private ApplicationContext context;

    @MockBean
    private ConfigValidator<T> validator;

    @MockBean
    private MessagePublisherUtil<T> messagePublisherUtil;

    @Autowired
    private KafkaProducerService<T> kafkaProducerService;

    @MockBean
    private ClaimsCheckServiceImpl<T> claimsCheckService;

    @Value("${spring.retry.maximum.attempts}")
    Integer retryCount;

    Map<String, Object> kafkaHeader;

    @BeforeEach
    void setUp() {
        kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
    }

    @Test
    void testPostingMessageOnTopic() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader);
        verify(validator, times(1)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testTopicNameValidationException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TopicNameValidationException.class).when(messagePublisherUtil).produceMessageToRetryOrDlt(
                any(TopicNameValidationException.class), anyMap(), (T) anyString(), anyMap());
        doThrow(TopicNameValidationException.class).when(validator).validateInputsForMultipleProducerFlow(anyMap(),
                (T) anyString());
        assertThrows(TopicNameValidationException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(validator, times(1)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testRetryWhenRuntimeException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TimeoutException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doThrow(TimeoutException.class).when(messagePublisherUtil)
                .produceMessageToRetryOrDlt(any(TimeoutException.class), anyMap(), (T) anyString(), anyMap());
        assertThrows(RuntimeException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(retryCount)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testNoRetryWhenRuntimeExceptionInClaimsCheck() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        topicMap.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, "claim");
        RecordTooLargeException recordTooLargeException=new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class),
                anyMap());
        kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader);
        // verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(anyMap(), anyString(), any());
    }

    @Test
    void testNoRetryWhenRuntimeExceptionInClaimsCheckAndClaimsCheckTopicNotPassed() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        RecordTooLargeException recordTooLargeException=new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class),
                anyMap());
        assertThrows(ClaimsCheckFailedException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        // verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(0)).handleClaimsCheckAfterGettingMemoryIssue(anyMap(), anyString(), any());

    }

    @Test
    void testRecoveryWhenTimeoutException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TimeoutException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doThrow(TimeoutException.class).when(messagePublisherUtil)
                .produceMessageToRetryOrDlt(any(TimeoutException.class), anyMap(), (T) anyString(), anyMap());
        assertThrows(TimeoutException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToRetryOrDlt(any(TimeoutException.class), anyMap(), any(),
                anyMap());
        verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(3)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testRetryTopic() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TopicNameValidationException.class).when(validator).validateInputsForMultipleProducerFlow(anyMap(),
                (T) anyString());
        kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader);
        verify(validator, times(1)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }
}
