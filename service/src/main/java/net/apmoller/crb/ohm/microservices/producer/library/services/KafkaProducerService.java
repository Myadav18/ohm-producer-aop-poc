package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.Map;

public interface KafkaProducerService<T> {

    void produceMessages(Map<String, String> topics, T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, KafkaServerNotFoundException, InternalServerException;
}
