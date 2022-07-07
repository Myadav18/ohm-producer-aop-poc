package net.apmoller.crb.ohm.microservices.producer.library.utils;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaHeaderValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private TimeoutException timeoutException = new TimeoutException("timeout");
    private TransactionTimedOutException tte = new TransactionTimedOutException("timeout");

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
    void testHeaderValidationExceptionWhenHeaderValueNull() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        KafkaHeaderValidationException ex = new KafkaHeaderValidationException(
                ConfigConstants.INVALID_KAFKA_HEADER_VALUE_ERROR_MSG);
        assertThrows(KafkaHeaderValidationException.class,
                () -> messagePublisherUtil.produceMessageToDlt(ex, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testInvalidTopicNameExceptionWhilePostingToMainTopic() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        TopicNameValidationException tnve = new TopicNameValidationException(
                ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        assertThrows(TopicNameValidationException.class,
                () -> messagePublisherUtil.produceMessageToDlt(tnve, topicMap, (T) payload, kafkaHeader));
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
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        messagePublisherUtil.produceMessageToDlt(timeoutException, topicMap, (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testInternalServerException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
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
                () -> messagePublisherUtil.produceMessageToDlt(tte, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void testSingleProducerDltTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.produceMessageToDlt(tte, (T) payload, kafkaHeader));
    }

    @Test
    void testSingleProducerRetryTransactionTimedOutException() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        assertThrows(TransactionTimedOutException.class,
                () -> messagePublisherUtil.produceMessageToDlt(tte, (T) payload, kafkaHeader));
    }

    @Test
    void testSingleProducerDltSuccessCase() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
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
        messagePublisherUtil.produceMessageToDlt(tte, (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerDltFailureCase() {
        String payload = "test";
        String producerTopic = "test";
        long offset = 1L;
        int partition = 2;
        Throwable ex = mock(Throwable.class);
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(producerTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(ex);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        assertThrows(Throwable.class, () -> messagePublisherUtil.produceMessageToDlt(tte, (T) payload, kafkaHeader));
    }

    @Test
    void testKafkaExceptionCase() {
        String payload = "test";
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        TransactionTimedOutException tte = new TransactionTimedOutException("");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RecordTooLargeException.class);
        assertThrows(RecordTooLargeException.class,
                () -> messagePublisherUtil.produceMessageToDlt(tte, (T) payload, kafkaHeader));
    }

    @Test
    void testFailurePostingToTopicWhenTopicAuthorizationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        TopicAuthorizationException topicAuthorizationException = new TopicAuthorizationException("test");
        KafkaException kafkaException = new KafkaException(topicAuthorizationException);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.FALSE);
        assertThrows(KafkaException.class,
                () -> messagePublisherUtil.produceMessageToDlt(kafkaException, topicMap, (T) payload, kafkaHeader));
        verify(kafkaTemplate, times(0)).send(any(ProducerRecord.class));
    }

    @Test
    void testMultipleProducerFailureToPostingOnDLTWhenInputValidationException() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        TopicNameValidationException topicNameValidationException = new TopicNameValidationException(
                ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG);
        when(configValidator.isInputValidationException(topicNameValidationException)).thenReturn(Boolean.TRUE);
        assertThrows(TopicNameValidationException.class, () -> messagePublisherUtil
                .produceMessageToDlt(topicNameValidationException, topicMap, (T) payload, kafkaHeader));
    }

    @Test
    void tesExceptionWhilePostingOnDLT() {
        Map<String, String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        String payload = "test";
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(RuntimeException.class);
        when(configValidator.dltTopicPresent(topicMap)).thenReturn(Boolean.TRUE);
        assertThrows(RuntimeException.class,
                () -> messagePublisherUtil.produceMessageToDlt(timeoutException, topicMap, (T) payload, kafkaHeader));
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
        messagePublisherUtil.produceMessageToDlt(timeoutException, topicMap, (T) payload, kafkaHeader);
        verify(configValidator, times(1)).dltTopicPresent(topicMap);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerExceptionOccuredWhilePostingOnRetryTopic() {
        String payload = "test";
        String RetryTopic = "retryTest";
        long offset = 1L;
        int partition = 2;
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.isInputValidationException(timeoutException)).thenReturn(Boolean.FALSE);
        when(configValidator.sendToRetryTopic(anyString(), any())).thenReturn(Boolean.TRUE);
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(RetryTopic, (T) "payload");
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(RetryTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.produceToRetryOrDlt(timeoutException, (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerPostingOnDltTopic() {
        String payload = "test";
        String RetryTopic = "retryTest";
        long offset = 1L;
        int partition = 2;
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.isInputValidationException(timeoutException)).thenReturn(Boolean.FALSE);
        when(configValidator.sendToRetryTopic(anyString(), any())).thenReturn(Boolean.FALSE);
        when(configValidator.dltTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(RetryTopic, (T) "payload");
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(RetryTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        messagePublisherUtil.produceToRetryOrDlt(timeoutException, (T) payload, kafkaHeader);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testSingleProducerExceptionWhilePostingOnDltTopic() {
        String payload = "test";
        String RetryTopic = "retryTest";
        long offset = 1L;
        int partition = 2;
        TimeoutException timeoutException = new TimeoutException("test exception");
        when(configValidator.isInputValidationException(timeoutException)).thenReturn(Boolean.FALSE);
        when(configValidator.sendToRetryTopic(anyString(), any())).thenReturn(Boolean.FALSE);
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(RetryTopic, (T) "payload");
        Throwable ex = mock(Throwable.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(RetryTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        assertThrows(Throwable.class,
                () -> messagePublisherUtil.produceToRetryOrDlt(timeoutException, (T) payload, kafkaHeader));
    }

    @Test
    void testAvroKafkaTemplate() {
        List<String> downstream = new ArrayList<>();
        downstream.add("documentservice");
        String testPayload = "{\n"
                + "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n"
                + "}";
        net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel avro = net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel
                .newBuilder().setResponse(testPayload).setCorrelationId("TESTCORRELATIONID1234567890")
                .setMessageType("xml").setMessageId("TESTMESSAGEID1234567890").setSourceSystem("docbroker")
                .setResponseConsumers(downstream).build();
        Schema schema = ReflectData.get().getSchema(avro.getClass());
        messagePublisherUtil.getKafkaTemplate(schema);
    }

    @Test
    void testSingleProducerPostingToDltTopic() {
        long offset = 1L;
        int partition = 2;
        TimeoutException timeoutException = new TimeoutException("test exception");
        String producerTopic = "test";
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T) "payload");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        when(configValidator.dltTopicIsPresent("dltTopic")).thenReturn(Boolean.TRUE);
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
        messagePublisherUtil.publishToRetryTopic((T) "TestMessage", kafkaHeader, null, "dltTopic", timeoutException);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testMutipleProducerPostingToDltTopic() {
        Map<String, String> topicMap = new HashMap<>();
        String payload = "test";
        long offset = 1L;
        int partition = 2;
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, null);
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
        messagePublisherUtil.publishMessageToRetryTopic((T) payload, kafkaHeader,
                new KafkaException(topicAuthorizationException), topicMap);
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

}
