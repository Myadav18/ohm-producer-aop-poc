package net.apmoller.crb.ohm.microservices.producer.library.services;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.DLTException;
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

    @MockBean
    private MeterRegistry registry;

    @Value("${spring.retry.maximum.attempts}")
    Integer retryCount;

    Map<String, Object> kafkaHeader;

    @BeforeEach
    void setUp() {
        kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        Counter counter = mock(Counter.class);
        when(registry.counter(any())).thenReturn(counter);
    }

    @Test
    void testPostingMessageOnTopic() throws IOException {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader);
        verify(validator, times(1)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testTopicNameValidationException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TopicNameValidationException.class).when(messagePublisherUtil)
                .produceMessageToDlt(any(TopicNameValidationException.class), anyMap(), (T) anyString(), anyMap());
        doThrow(TopicNameValidationException.class).when(validator).validateInputsForMultipleProducerFlow(anyMap(),
                (T) anyString());
        assertThrows(TopicNameValidationException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(validator, times(1)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testRetryWhenRetriableException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TimeoutException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doThrow(TimeoutException.class).when(messagePublisherUtil).produceMessageToDlt(any(TimeoutException.class),
                anyMap(), (T) anyString(), anyMap());
        assertThrows(RuntimeException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(retryCount)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testWhenRuntimeExceptionInClaimsCheckAndTopicPassed() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        topicMap.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, "claim");
        RecordTooLargeException recordTooLargeException = new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doNothing().when(claimsCheckService).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);
        kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);
    }

    @Test
    void testWhenRuntimeExceptionInClaimsCheckAndTopicNotPassed() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        // topicMap.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, "claim");
        RecordTooLargeException recordTooLargeException = new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doThrow(ClaimsCheckFailedException.class).when(claimsCheckService)
                .handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap, (T) payload);
        assertThrows(ClaimsCheckFailedException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);
    }

    @Test
    void testRecoveryWhenTimeoutException() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        doThrow(TimeoutException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        doThrow(TimeoutException.class).when(messagePublisherUtil).produceMessageToDlt(any(TimeoutException.class),
                anyMap(), (T) anyString(), anyMap());
        assertThrows(TimeoutException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToDlt(any(TimeoutException.class), anyMap(), any(),
                anyMap());
        verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(3)).publishOnTopic(any(ProducerRecord.class), anyMap());

        doNothing().when(messagePublisherUtil).produceMessageToDlt(any(TimeoutException.class),
                anyMap(), (T) anyString(), anyMap());
        assertThrows(DLTException.class,
                () -> kafkaProducerService.produceMessages(topicMap, (T) payload, kafkaHeader));
    }
}
