package net.apmoller.crb.ohm.microservices.producer.library.utils;

import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
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
@SpringBootTest(classes = {MessagePublisherUtil.class })
@ActiveProfiles({ "test" })
public class MessagePublisherUtilTest <T> {

    @MockBean
    private KafkaTemplate kafkaTemplate;

    @Mock
    SendResult<String, Object> sendResult;

    @Mock
    ListenableFuture<SendResult<String, Object>> responseFuture;

    @Autowired
    private MessagePublisherUtil messagePublisherUtil;

    @Test
    void testSuccessfulPublishOnTopic()
    {
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T)"payload");
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
    void testFailureOnPublishToTopic()
    {
        long offset = 1L;
        int partition = 2;
        String producerTopic = "test";
        Map<String, Object> kafkaHeader = new HashMap<>();
        kafkaHeader.put("X-DOCBROKER-Correlation-ID", "DUMMYHEXID");
        ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, (T)"payload");
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
        assertThrows(InternalServerException.class, () -> messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader));
        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void testGetErrorTopicWhenTimeOutException()
    {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        var errorTopic = messagePublisherUtil.getErrorTopic(new TimeoutException(), topicMap);
        assertEquals("retry", errorTopic);
    }

    @Test
    void testGetErrorTopicWhenNullPointerException()
    {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        var errorTopic = messagePublisherUtil.getErrorTopic(new NullPointerException(), topicMap);
        assertEquals("dlt", errorTopic);
    }

    @Test
    void testGetErrorTopicWhenKafkaServerNotFoundException()
    {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.NOTIFICATION_TOPIC_KEY, "test-topic");
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertThrows(KafkaServerNotFoundException.class, () ->
                messagePublisherUtil.getErrorTopic(new KafkaServerNotFoundException(ConfigConstants.INVALID_BOOTSTRAP_SERVER_ERROR_MSG), topicMap));
    }

    @Test
    void testGetErrorTopicWhenInvalidTopicException()
    {
        Map<String,String> topicMap = new HashMap<>();
        topicMap.put(ConfigConstants.RETRY_TOPIC_KEY, "retry");
        topicMap.put(ConfigConstants.DEAD_LETTER_TOPIC_KEY, "dlt");
        assertThrows(InvalidTopicException.class, () ->
                messagePublisherUtil.getErrorTopic(new InvalidTopicException(ConfigConstants.INVALID_NOTIFICATION_TOPIC_ERROR_MSG), topicMap));
    }

}
