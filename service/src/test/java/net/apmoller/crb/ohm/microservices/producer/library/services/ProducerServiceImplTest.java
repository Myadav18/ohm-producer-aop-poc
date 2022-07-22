package net.apmoller.crb.ohm.microservices.producer.library.services;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.DLTException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.TransactionTimedOutException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@Slf4j
@SpringBootTest(classes = { ProducerServiceImpl.class })
@ActiveProfiles({ "test" })
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ProducerServiceImplTest<T> {

    @MockBean
    private ApplicationContext context;

    @MockBean
    private Environment environment;

    @MockBean
    private MeterRegistry registry;

    @MockBean
    private ConfigValidator<T> validate;

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @MockBean
    private ClaimsCheckServiceImpl<T> claimsCheckService;

    @MockBean
    private TopicNameValidationException topicNameValidationException;

    @MockBean
    private KafkaServerNotFoundException kafkaServerNotFoundException;

    @MockBean
    private NullPointerException nullPointerException;

    @MockBean
    private SerializationException serializationException;

    @Autowired
    private ProducerServiceImpl<T> producerServiceImpl;

    @MockBean
    private MessagePublisherUtil<T> messagePublisherUtil;

    private Map<String, Object> kafkaHeader;

    private final String message = "test";

    private TimeoutException timeoutException = new TimeoutException("timeout");
    private TransactionTimedOutException transactionTimedOutException = new TransactionTimedOutException("timeout");

    @BeforeEach
    void setUp() {
        kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        Counter counter = mock(Counter.class);
        when(registry.counter(any())).thenReturn(counter);
    }

    @Test
    void testMessageSentToTopic() throws IOException {
        producerServiceImpl.produceMessages((T) message, kafkaHeader);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testMessageSentToTopicFailure() {
        doThrow(RuntimeException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        assertThrows(RuntimeException.class, () -> producerServiceImpl.produceMessages((T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testTopicNotFound() {
        doThrow(TopicNameValidationException.class).when(validate).validateInputs(any(), any());
        assertThrows(TopicNameValidationException.class,
                () -> producerServiceImpl.produceMessages((T) message, kafkaHeader));
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testKafkaServerNotFoundException() {
        doThrow(KafkaServerNotFoundException.class).when(validate).validateInputs(any(), any());
        assertThrows(KafkaServerNotFoundException.class,
                () -> producerServiceImpl.produceMessages((T) message, kafkaHeader));
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testProducerServiceForRecover() {
        assertThrows(DLTException.class, () -> producerServiceImpl
                .publishMessageOnDltTopic(transactionTimedOutException, (T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToDlt(any(), any(), anyMap());
    }

    @Test
    void testRecoverInvalidTopicNameExceptionTest() {
        doThrow(topicNameValidationException).when(messagePublisherUtil)
                .produceMessageToDlt(topicNameValidationException, (T) message, kafkaHeader);
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnDltTopic(topicNameValidationException, (T) message, kafkaHeader));
    }

    @Test
    void testRecoverTransactionTImeOutException() {
        assertThrows(DLTException.class, () -> producerServiceImpl
                .publishMessageOnDltTopic(transactionTimedOutException, (T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToDlt(transactionTimedOutException, (T) message,
                kafkaHeader);
    }

    @Test
    void testRecoverForTimeOutException() {
        assertThrows(DLTException.class,
                () -> producerServiceImpl.publishMessageOnDltTopic(timeoutException, (T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToDlt(timeoutException, (T) message, kafkaHeader);
    }

    @Test
    void testInvalidMainTopic() {
        doThrow(topicNameValidationException).when(messagePublisherUtil)
                .produceMessageToDlt(topicNameValidationException, (T) message, kafkaHeader);
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnDltTopic(topicNameValidationException, (T) message, kafkaHeader));
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testTopicAuthorizationException() {
        org.apache.kafka.common.KafkaException kafkaException = new org.apache.kafka.common.KafkaException(
                new TopicAuthorizationException("test"));
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        assertThrows(org.apache.kafka.common.KafkaException.class,
                () -> producerServiceImpl.produceMessages((T) message, kafkaHeader));
    }

    @Test
    void testPostingOnDLTWhenTopicAuthorizationException() {
        when(environment.resolvePlaceholders(ConfigConstants.DLT))
                .thenReturn("${kafka.notification.dead-letter-topic}");
        when(context.getEnvironment()).thenReturn(environment);
        org.apache.kafka.common.KafkaException kafkaException = new org.apache.kafka.common.KafkaException(
                new TopicAuthorizationException("test"));
        doNothing().when(messagePublisherUtil).produceMessageToDlt(kafkaException, (T) message, kafkaHeader);
        assertThrows(DLTException.class,
                () -> producerServiceImpl.publishMessageOnDltTopic(kafkaException, (T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceMessageToDlt(kafkaException, (T) message, kafkaHeader);
    }

    @Test
    void testWhenRuntimeExceptionAndClaimsCheckTopicPassed() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, "${kafka.notification.claimscheck-topic}");
        topicMap.put(ConfigConstants.CLAIMS_CHECK_DLT_KEY, "${kafka.notification.claimscheck-dlt}");
        RecordTooLargeException recordTooLargeException = new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doNothing().when(claimsCheckService).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        producerServiceImpl.produceMessages((T) payload, kafkaHeader);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);

    }

    @Test
    void testWhenRuntimeExceptionAndClaimsCheckTopicNotPassed() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, "${kafka.notification.claimscheck-topic}");
        topicMap.put(ConfigConstants.CLAIMS_CHECK_DLT_KEY, "${kafka.notification.claimscheck-dlt}");
        RecordTooLargeException recordTooLargeException = new RecordTooLargeException("record too large");
        KafkaException kafkaException = new KafkaException(recordTooLargeException);
        doThrow(ClaimsCheckFailedException.class).when(claimsCheckService)
                .handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap, (T) payload);
        doThrow(kafkaException).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        assertThrows(ClaimsCheckFailedException.class,
                () -> producerServiceImpl.produceMessages((T) payload, kafkaHeader));
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topicMap,
                (T) payload);

    }
}
