package net.apmoller.crb.ohm.microservices.producer.library.utils;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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

    @Test
    void testSuccessfulPublishOnTopic() {
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
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
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
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
        assertThrows(InternalServerException.class,
                () -> messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader));
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testGetErrorTopicWhenTimeOutException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        Assertions.assertThrows(TimeoutException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(new TimeoutException(), topicMap, (T) payload, new HashMap<>()));

    }

    @Test
    void testGetErrorTopicWhenNullPointerException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        Assertions.assertThrows(NullPointerException.class, () -> messagePublisherUtil
                .produceMessageToRetryOrDlt(new NullPointerException(), topicMap, (T) payload, new HashMap<>()));
        // assertEquals("dlt", errorTopic);
    }

    @Test
    void testGetErrorTopicWhenKafkaServerNotFoundException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertThrows(KafkaServerNotFoundException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(
                        new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG), topicMap,
                        (T) payload, new HashMap<>()));
    }

    @Test
    void testGetErrorTopicWhenInvalidTopicNameException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertThrows(TopicNameValidationException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(
                        new TopicNameValidationException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG),
                        topicMap, (T) payload, new HashMap<>()));
    }

    @Test
    void testTransactionTimedOutException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(new TransactionTimedOutException(""), topicMap,
                        (T) payload, new HashMap<>()));
    }

    @Test
    void testTimeoutException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
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
        when(configValidator.retryTopicPresent(anyMap())).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        messagePublisherUtil.produceMessageToRetryOrDlt(new TimeoutException(""), topicMap, (T) payload,
                new HashMap<>());
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
        assertThrows(InternalServerException.class,
                () -> messagePublisherUtil.produceMessageToRetryOrDlt(new TransactionTimedOutException(""), topicMap,
                        (T) payload, new HashMap<>()));
    }

    @Test
    void testRetrySuccessCase() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        when(configValidator.retryTopicPresent(anyMap())).thenReturn(Boolean.TRUE);
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
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
        messagePublisherUtil.produceMessageToRetryOrDlt(new TimeoutException(""), topicMap, (T) payload,
                new HashMap<>());
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerDltTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class, () -> messagePublisherUtil.publishToDltTopic((T) payload,
                new HashMap<>(), "dltTestTopic", new TransactionTimedOutException("")));
    }

    @Test
    void testSingleProducerRetryTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class, () -> messagePublisherUtil.publishToRetryTopic((T) payload,
                new HashMap<>(), "retryTestTopic", "dltTopic", new TransactionTimedOutException("")));
    }

    @Test
    void testSingleProducerRetrySuccessCase() {
        String payload = "test";
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
        messagePublisherUtil.publishToRetryTopic((T) payload, new HashMap<>(), "retryTopic", "dltTopic",
                new TransactionTimedOutException(""));
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
        messagePublisherUtil.publishToDltTopic((T) payload, new HashMap<>(), "dltTestTopic",
                new TransactionTimedOutException(""));
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
        assertThrows(InternalServerException.class, () -> messagePublisherUtil.publishToDltTopic((T) payload,
                new HashMap<>(), "dltTestTopic", new TransactionTimedOutException("")));
    }

}
