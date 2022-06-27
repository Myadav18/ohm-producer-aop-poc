package net.apmoller.crb.ohm.microservices.producer.library.utils;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaHeaderValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { MessagePublisherUtil.class })
@ActiveProfiles({ "test" })
public class MessagePublisherUtilTest<T> {

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @MockBean
    private ConfigValidator<T> configValidator;

    @Mock
    SendResult<String, Object> sendResult;

    @Mock
    ListenableFuture<SendResult<String, Object>> responseFuture;

    @Autowired
    private MessagePublisherUtil<T> messagePublisherUtil;

    @MockBean
    private ApplicationContext context;

    @MockBean
    private Environment environment;

    Map<String, Object> kafkaHeader;

    private final TransactionTimedOutException tte = new TransactionTimedOutException("test exception");

    @BeforeEach
    void setUp() {
        kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
    }

    @Test
    void testSuccessfulPublishOnTopic() {
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testFailureOnPublishToTopic() {
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(ex);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        assertThrows(Throwable.class, () -> messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader));
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testExceptionWhilePostingToRetry() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        TimeoutException timeoutException = new TimeoutException();
        Assertions.assertThrows(TimeoutException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(timeoutException, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testHeaderValidationExceptionWhilePostingToRetry() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        KafkaHeaderValidationException ex = new KafkaHeaderValidationException(
                ConfigConstants.INVALID_KAFKA_HEADER_MAP_ERROR_MSG);
        var throwable = assertThrows(KafkaHeaderValidationException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(ex, topicMap, (T) payload, null));
        Assertions.assertEquals(ConfigConstants.INVALID_KAFKA_HEADER_MAP_ERROR_MSG, throwable.getMessage());
    }

    @Test
    void testHeaderValidationExceptionWhenHeaderValueNull() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        KafkaHeaderValidationException ex = new KafkaHeaderValidationException(
                ConfigConstants.INVALID_KAFKA_HEADER_VALUE_ERROR_MSG);
        assertThrows(KafkaHeaderValidationException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(ex, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testExceptionWhilePostingToDlt() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        NullPointerException nullPointerException = new NullPointerException();
        Assertions.assertThrows(NullPointerException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(nullPointerException, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testKafkaServerNotFoundException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        KafkaServerNotFoundException kafkaServerNotFoundException = new KafkaServerNotFoundException(
                ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG);
        assertThrows(KafkaServerNotFoundException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(kafkaServerNotFoundException, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testInvalidTopicNameExceptionWhilePostingToMainTopic() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        TopicNameValidationException tnve = new TopicNameValidationException(
                ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        assertThrows(TopicNameValidationException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(tnve, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testTransactionTimedOutException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(tte, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testTimeoutException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        when(configValidator.retryTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        messagePublisherUtil.produceMessageToRetryOrDlt(new TimeoutException(""), topicMap, (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testInternalServerException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        when(configValidator.dltTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(ex);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        assertThrows(Throwable.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(tte, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testRetryWhenRetryTopicValidationFailed() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String producerTopic = "test";
        when(configValidator.sendToRetryTopic(topicMap, new TimeoutException())).thenReturn(Boolean.FALSE);
        assertThrows(TimeoutException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(new TimeoutException(""), topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testSingleProducerDltTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.publishToDltTopic((T) payload, null, "dltTestTopic", tte));
    }

    @Test
    void testSingleProducerRetryTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.publishToRetryTopic((T) payload, null, "retryTestTopic", "dltTopic", tte));
    }

    @Test
    void testSingleProducerRetrySuccessCase() {
        String payload = "test";
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.publishToRetryTopic((T) payload, kafkaHeader, "retryTopic", "dltTopic", tte);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerDltSuccessCase() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.publishToDltTopic((T) payload, kafkaHeader, "dltTestTopic", tte);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerDltFailureCase() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(ex);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        assertThrows(Throwable.class,
                () -> messagePublisherUtil.publishToDltTopic((T) payload, kafkaHeader, "dltTestTopic", tte));
    }

    @Test
    void testKafkaExceptionCase() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        TransactionTimedOutException tte = new TransactionTimedOutException("");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RecordTooLargeException.class);
        assertThrows(RecordTooLargeException.class,
                () -> messagePublisherUtil.publishToDltTopic((T) payload, kafkaHeader, "dltTestTopic", tte));
    }

    @Test
    void testPostingOnRetryWhenTopicAuthorizationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        TopicAuthorizationException topicAuthorizationException = new TopicAuthorizationException("test");
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(topicMap.get(ConfigConstants.RETRY_TOPIC_KEY), partition), offset, 0L, 0L, 0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.publishMessageToRetryTopic((T) payload, kafkaHeader,
                new KafkaException(topicAuthorizationException), topicMap);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testPostingOnDLTWhenTopicAuthorizationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        TopicAuthorizationException topicAuthorizationException = new TopicAuthorizationException("test");
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(topicMap.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY), partition), offset, 0L, 0L, 0L,
                0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.produceMessageToRetryOrDlt(new KafkaException(topicAuthorizationException), topicMap,
                (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testExceptionPostingOnDLTWhenTopicAuthorizationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RuntimeException.class);
        TopicAuthorizationException topicAuthorizationException = new TopicAuthorizationException("test");
        assertThrows(RuntimeException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(new KafkaException(topicAuthorizationException),
                        topicMap, (T) payload, kafkaHeader));
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testFailurePostingToTopicWhenTopicAuthorizationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        TopicAuthorizationException topicAuthorizationException = new TopicAuthorizationException("test");
        KafkaException kafkaException = new KafkaException(topicAuthorizationException);
        when(configValidator.retryTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        assertThrows(KafkaException.class, () -> messagePublisherUtil.produceMessageToRetryOrDlt(kafkaException,
                topicMap, (T) payload, kafkaHeader));
        verify(kafkaTemplate, times(0)).send(any(ProducerRecord.class));
    }

    @Test
    void testMultipleProducerFailureToPostingOnRetryOrDLTWhenInputValidationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        TopicNameValidationException topicNameValidationException = new TopicNameValidationException(
                ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        when(configValidator.isInputValidationException(topicNameValidationException)).thenReturn(Boolean.TRUE);
        assertThrows(TopicNameValidationException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(topicNameValidationException, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testSingleProducerFailureToPostingOnRetryOrDLTWhenInputValidationException() {
        String payload = "test";
        TopicNameValidationException topicNameValidationException = new TopicNameValidationException(
                ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        when(configValidator.isInputValidationException(topicNameValidationException)).thenReturn(Boolean.TRUE);
        assertThrows(TopicNameValidationException.class,
                () -> messagePublisherUtil.produceToRetryOrDlt(topicNameValidationException, (T) payload, kafkaHeader));
    }

    @Test
    void testMultipleProducerFailureToPostingOnRetryOrDLTWhenKafkaHeaderNotPresent() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String payload = "test";
        Map<String, Object> headerMap = new HashMap<>();
        headerMap.put("X-Correlation-Id", null);
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.sendToRetryTopic(topicMap, timeoutException)).thenReturn(Boolean.TRUE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        assertThrows(KafkaHeaderValidationException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(timeoutException, topicMap, (T) payload, headerMap));
    }

    @Test
    void tesExceptionWhilePostingOnRetryOrDLT() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String payload = "test";
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.sendToRetryTopic(topicMap, timeoutException)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RuntimeException.class);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        assertThrows(RuntimeException.class, () -> messagePublisherUtil.produceMessageToRetryOrDlt(timeoutException,
                topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void tesExceptionWhenPublishingToRetryTopicForMultipleProducer() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String payload = "test";
        Map<String, Object> headerMap = new HashMap<>();
        headerMap.put("X-Correlation-Id", null);
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RuntimeException.class);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        assertThrows(RuntimeException.class, () -> messagePublisherUtil.publishMessageToRetryTopic((T) payload,
                kafkaHeader, timeoutException, topicMap));
    }

    @Test
    void tesExceptionWhenPublishingToRetryTopicForSingleProducer() {
        String payload = "test";
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RuntimeException.class);
        when(configValidator.dltTopicIsPresent("dlt")).thenReturn(Boolean.FALSE);
        assertThrows(RuntimeException.class, () -> messagePublisherUtil.publishToRetryTopic((T) payload, kafkaHeader,
                "retry", "dlt", timeoutException));
    }

    @Test
    void testSuccessfulPostingToRetryTopic() {
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.sendToRetryTopic(topicMap, timeoutException)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(topicMap.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY), partition), offset, 0L, 0L, 0L,
                0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.produceMessageToRetryOrDlt(timeoutException, topicMap, (T) payload, kafkaHeader);
        verify(configValidator, times(1)).sendToRetryTopic(topicMap, timeoutException);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSuccessfulPostingToDLT() {
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.sendToRetryTopic(topicMap, timeoutException)).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(topicMap.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY), partition), offset, 0L, 0L, 0L,
                0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.produceMessageToRetryOrDlt(timeoutException, topicMap, (T) payload, kafkaHeader);
        verify(configValidator, times(1)).sendToRetryTopic(topicMap, timeoutException);
        verify(configValidator, times(1)).dltTopicPresent(topicMap);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

}
