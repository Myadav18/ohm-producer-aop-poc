package net.apmoller.crb.ohm.microservices.kafkaproducer.producer;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;

/**
 * This is Kafka message generator interface that provides method to generate message payload that would be published
 * from Kafka.
 */
public interface KafkaMessageGenerator {
    /**
     * @param user
     *            - an object model data
     * 
     * @return Kafka messaged payload.
     */
    String getMessage(User user);
}
