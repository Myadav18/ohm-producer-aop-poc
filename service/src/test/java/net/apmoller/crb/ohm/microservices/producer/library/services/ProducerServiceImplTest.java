package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@Slf4j
@SpringBootTest(classes = { ProducerServiceImpl.class })
@ActiveProfiles({ "test" })
public class ProducerServiceImplTest<T> {

    @MockBean
    private ApplicationContext context;

    @MockBean
    private Environment environment;

    @MockBean
    private ConfigValidator<T> validate;

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @MockBean
    private TimeoutException timeoutException;

    @MockBean
    private ClaimsCheckService<T> claimsCheckService;

    @MockBean
    private TransactionTimedOutException transactionTimedOutException;

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

    @BeforeEach
    void setUp() {
        kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
    }

    @Test
    void testMessageSentToTopic() {
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
        producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, (T) message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToDltTopic(any(), anyMap(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testRecoverInvalidTopicNameExceptionTest() {
        doThrow(topicNameValidationException).when(messagePublisherUtil)
                .produceToRetryOrDlt(topicNameValidationException, (T) message, kafkaHeader);
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnRetryOrDltTopic(topicNameValidationException, (T) message, kafkaHeader));
        verify(messagePublisherUtil, times(1)).produceToRetryOrDlt(topicNameValidationException, (T) message,
                kafkaHeader);
    }

    @Test
    void testRecoverTransactionTImeOutException() {
        when(validate.retryTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, (T) message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToRetryTopic(any(), anyMap(), anyString(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testRecoverForTimeOutException() {
        when(validate.retryTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        producerServiceImpl.publishMessageOnRetryOrDltTopic(timeoutException, (T) message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToRetryTopic(any(), anyMap(), anyString(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testInvalidMainTopic() {
        doThrow(topicNameValidationException).when(messagePublisherUtil)
                .produceToRetryOrDlt(topicNameValidationException, (T) message, kafkaHeader);
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnRetryOrDltTopic(topicNameValidationException, (T) message, kafkaHeader));
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
    void testPostingOnRetryTopicWhenTopicAuthorizationException() {
        when(environment.resolvePlaceholders(ConfigConstants.RETRY_TOPIC))
                .thenReturn("${kafka.notification.retry-topic}");
        when(context.getEnvironment()).thenReturn(environment);
        org.apache.kafka.common.KafkaException kafkaException = new org.apache.kafka.common.KafkaException(
                new TopicAuthorizationException("test"));
        doNothing().when(messagePublisherUtil).produceToRetryOrDlt(kafkaException, (T) message, kafkaHeader);
        producerServiceImpl.publishMessageOnRetryOrDltTopic(kafkaException, (T) message, kafkaHeader);
        verify(messagePublisherUtil, times(1)).produceToRetryOrDlt(kafkaException, (T) message, kafkaHeader);
    }


    @Test
    void testNoRetryWhenRuntimeExceptionInClaimsCheckAndClaimsCheckTopicNotPassed1() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        when(environment.resolvePlaceholders(ConfigConstants.CLAIMS_CHECK))
                .thenReturn("${kafka.notification.claimscheck-topic}");
        when(context.getEnvironment()).thenReturn(environment);
        when(validate.claimsCheckTopicIsPresent(any())).thenReturn(true);
        doThrow(RecordTooLargeException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        producerServiceImpl.produceMessages(any(), anyMap());
        // verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(1)).handleClaimsCheckAfterGettingMemoryIssue(anyMap(),anyString(),any());

    }
    @Test
    void testNoRetryWhenRuntimeExceptionInClaimsCheckAndClaimsCheckTopicPassed1() {
        String payload = "test";
        Map<String, String> topicMap = new HashMap<>();
        when(environment.resolvePlaceholders(ConfigConstants.CLAIMS_CHECK))
                .thenReturn("${kafka.notification.claimscheck-topic}");
        when(context.getEnvironment()).thenReturn(environment);
        when(validate.claimsCheckTopicIsPresent(any())).thenReturn(false);
        doThrow(RecordTooLargeException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        assertThrows(ClaimsCheckFailedException.class,
                () ->producerServiceImpl.produceMessages(any(), anyMap()));
        // verify(validator, times(retryCount)).validateInputsForMultipleProducerFlow(topicMap, (T) payload);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
        verify(claimsCheckService, times(0)).handleClaimsCheckAfterGettingMemoryIssue(anyMap(),anyString(),any());

    }
}
