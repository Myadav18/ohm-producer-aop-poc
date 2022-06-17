package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
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
public class ProducerServiceImplTest {

    @MockBean
    private ApplicationContext context;

    @MockBean
    private ConfigValidator<T> validate;

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @MockBean
    private TimeoutException timeoutException;

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
    private ProducerServiceImpl producerServiceImpl;

    @MockBean
    private MessagePublisherUtil<T> messagePublisherUtil;

    @Test
    void testMessageSentToTopic() throws IOException {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        producerServiceImpl.produceMessages(message, kafkaHeader);
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testMessageSentToTopicFailure() {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        doThrow(RuntimeException.class).when(messagePublisherUtil).publishOnTopic(any(ProducerRecord.class), anyMap());
        assertThrows(RuntimeException.class, () -> producerServiceImpl.produceMessages(message, new HashMap<>()));
        verify(messagePublisherUtil, times(1)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testTopicNotFound() throws TopicNameValidationException {
        String message = "test Message";
        String producerTopic = "";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        doThrow(TopicNameValidationException.class).when(validate).validateInputs(any(), any());
        assertThrows(TopicNameValidationException.class,
                () -> producerServiceImpl.produceMessages(message, kafkaHeader));
        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testKafkaServerNotFoundException() throws KafkaException {
        String message = "test Message";
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        doThrow(KafkaServerNotFoundException.class).when(validate).validateInputs(any(), any());
        assertThrows(KafkaServerNotFoundException.class,
                () -> producerServiceImpl.produceMessages(message, kafkaHeader));

        verify(messagePublisherUtil, times(0)).publishOnTopic(any(ProducerRecord.class), anyMap());
    }

    @Test
    void testProducerServiceForRecover() throws IOException {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToDltTopic(any(), anyMap(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testRecoverInvalidTopicNameExceptionTest() throws IOException {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnRetryOrDltTopic(topicNameValidationException, message, kafkaHeader));
    }

    @Test
    void testRecoverTransactionTImeOutException() throws IOException {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        when(validate.retryTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        producerServiceImpl.publishMessageOnRetryOrDltTopic(transactionTimedOutException, message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToRetryTopic(any(), anyMap(), anyString(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testRecoverForTimeOutException() throws IOException {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        when(validate.retryTopicIsPresent(anyString())).thenReturn(Boolean.TRUE);
        producerServiceImpl.publishMessageOnRetryOrDltTopic(timeoutException, message, kafkaHeader);
        verify(messagePublisherUtil, times(0)).publishToRetryTopic(any(), anyMap(), anyString(), anyString(),
                any(TopicNameValidationException.class));
    }

    @Test
    void testMessageSentToRetryTopicFailure() {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        when(validate.retryTopicIsPresent(anyString())).thenReturn(Boolean.FALSE);
        doThrow(NullPointerException.class).when(messagePublisherUtil).publishToDltTopic(any(), anyMap(), anyString(),
                any(NullPointerException.class));
        assertThrows(NullPointerException.class,
                () -> producerServiceImpl.publishMessageOnRetryOrDltTopic(nullPointerException, message, kafkaHeader));

        verify(messagePublisherUtil, times(1)).publishToDltTopic(any(), anyMap(), anyString(),
                any(NullPointerException.class));
    }

    @Test
    void testDeadLetterTopicNotFound() {
        String message = "test Message";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnRetryOrDltTopic(topicNameValidationException, message, kafkaHeader));
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }

    @Test
    void testInvalidMainTopic() throws TopicNameValidationException {
        String message = "test Message";
        String deadLetterTopic = "";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        assertThrows(TopicNameValidationException.class, () -> producerServiceImpl
                .publishMessageOnRetryOrDltTopic(topicNameValidationException, message, kafkaHeader));
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
        } catch (KafkaServerNotFoundException e) {
            log.info("Invalid Kafka bootStrap Server");
        }
        Mockito.verify(kafkaTemplate, times(0)).send((ProducerRecord) any());
    }
}
