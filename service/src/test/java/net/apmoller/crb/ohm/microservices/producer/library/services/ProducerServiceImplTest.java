package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.poi.ss.formula.functions.T;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@Slf4j
@SpringBootTest(classes = { ProducerServiceImpl.class })
@ActiveProfiles({ "test" })
public class ProducerServiceImplTest {

    @MockBean
    private ApplicationContext context;

    @MockBean
    private ConfigValidator validate;

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @MockBean
    private TimeoutException timeoutException;

    @MockBean
    private TransactionTimedOutException transactionTimedOutException;

    @MockBean
    private InvalidTopicException invalidTopicException;

    @MockBean
    private KafkaServerNotFoundException kafkaServerNotFoundException;

    @MockBean
    private NullPointerException nullPointerException;

    @MockBean
    private SerializationException serializationException;

    @Autowired
    private ProducerServiceImpl producerServiceImpl;

    @Test
    void testMessageSentToTopic() throws IOException {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
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
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        producerServiceImpl.produceMessages(message, kafkaHeader);
        Mockito.verify(kafkaTemplate, times(1)).send((ProducerRecord) any());
    }

    @Test
    void testMessageSentToTopicFailure() throws InternalServerException {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        Throwable throwable = mock(Throwable.class);
        given(throwable.getMessage()).willReturn(message);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(throwable);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        try {
            producerServiceImpl.produceMessages(message, kafkaHeader);
        } catch (InternalServerException | IOException e) {
            log.info("Message can't be published to kafka topic topic");
        }
        Mockito.verify(kafkaTemplate, times(1)).send((ProducerRecord) any());
    }

    @Test
    void testTopicNotFound() throws InvalidTopicException {
        String message = "test Message";
        String producerTopic = "";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            doThrow(InvalidTopicException.class).when(validate).validateInputs(any());
            producerServiceImpl.produceMessages(message, kafkaHeader);
        } catch (InvalidTopicException | IOException e) {
            log.info("Message can't be published to kafka topic topic");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testKafkaServerNotFoundException() throws KafkaException {
        String message = "test Message";
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        try {
            doThrow(KafkaServerNotFoundException.class).when(validate).validateInputs(any());
            producerServiceImpl.produceMessages(message, kafkaHeader);
        } catch (KafkaServerNotFoundException | IOException e) {
            log.info("Unable to Connect the Kafka Server");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testProducerServiceForRecover() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String retryTopic = "retryTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(retryTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");

        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        } catch (Exception e) {
            log.info("retry test error will come as expected");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverInvalidTopicExceptionTest() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String retryTopic = "retryTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(InvalidTopicException.class);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        } catch (InvalidTopicException | IOException e) {
            log.info("Topic placeholder is Not correct or it's Empty");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverTransactionTImeOutException() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String retryTopic = "retryTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(TransactionTimedOutException.class);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        } catch (TransactionTimedOutException | IOException e) {
            log.info("TransactionTimedOutException Occured");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverForTimeOutException() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String retryTopic = "retryTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(retryTopic, partition), offset, 0L, 0L,
                0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");

        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(timeoutException, message, kafkaHeader);
        } catch (Exception e) {
            log.info("retry test error will come as expected in case of timeoutException");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testMessageSentToRetryTopicFailure() throws InternalServerException {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
        Throwable throwable = mock(Throwable.class);
        given(throwable.getMessage()).willReturn(message);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onFailure(throwable);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        try {
            producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        } catch (InternalServerException | IOException e) {
            log.info("Message can't be published to kafka Retry topic topic");
        }
        Mockito.verify(kafkaTemplate, times(1)).send((ProducerRecord) any());
    }

    @Test
    void testRecoverException() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String retryTopic = "retryTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(Exception.class);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        } catch (Exception e) {
            log.info("Exception Occured");
        }
        verify(kafkaTemplate, times(0)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverForNullPointerException() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String deadLetterTopic = "dltTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(deadLetterTopic, partition), offset, 0L,
                0L, 0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");

        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(nullPointerException, message, kafkaHeader);
        } catch (Exception e) {
            log.info("retry test error will come as expected in case of nullPointerException");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverForSerailizationException() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String deadLetterTopic = "dltTest";
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(deadLetterTopic, partition), offset, 0L,
                0L, 0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");

        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(responseFuture);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(serializationException, message, kafkaHeader);
        } catch (Exception e) {
            log.info("retry test error will come as expected in case of serializationException");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testRecoverForSerailizationExceptionValidateTopic() {
        String message = "test Message";
        long offset = 1L;
        int partition = 2;
        String deadLetterTopic = null;
        SendResult<String, Object> sendResult = mock(SendResult.class);
        ListenableFuture<SendResult<String, T>> responseFuture = mock(ListenableFuture.class);

        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(deadLetterTopic, partition), offset, 0L,
                0L, 0L, 0, 0);
        given(sendResult.getRecordMetadata()).willReturn(recordMetadata);
        doAnswer(invocationOnMock -> {
            ListenableFutureCallback listenableFutureCallback = invocationOnMock.getArgument(0);
            listenableFutureCallback.onSuccess(sendResult);
            assertEquals(sendResult.getRecordMetadata().offset(), offset);
            assertEquals(sendResult.getRecordMetadata().partition(), partition);
            return null;
        }).when(responseFuture).addCallback(any(ListenableFutureCallback.class));
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenThrow(InvalidTopicException.class);
            producerServiceImpl.publishMessageOnRetryOrDltTopic(serializationException, message, kafkaHeader);
        } catch (InvalidTopicException | IOException e) {
            log.info("Topic placeholder is Not correct or it's Empty");
        }
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testDeadLetterTopicNotFound() {
        String message = "test Message";
        String deadLetterTopic = "test";
        String retryTopic = "test";
        String dltTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            producerServiceImpl.publishMessageOnRetryOrDltTopic(invalidTopicException, message, kafkaHeader);
        } catch (InvalidTopicException | IOException e) {
            log.info("Message can't be published to kafka topic topic");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testInvalidMainTopic() throws InvalidTopicException {
        String message = "test Message";
        String deadLetterTopic = "";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            producerServiceImpl.publishMessageOnRetryOrDltTopic(invalidTopicException, message, kafkaHeader);
        } catch (InvalidTopicException | IOException e) {
            log.info("Message can't be published to kafka topic topic");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testInvalidBootstarpServer() {
        String message = "test Message";
        String deadLetterTopic = "";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        try {
            producerServiceImpl.publishMessageOnRetryOrDltTopic(kafkaServerNotFoundException, message, kafkaHeader);
        } catch (KafkaServerNotFoundException | IOException e) {
            log.info("Invalid Kafka bootStrap Server");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }
}
