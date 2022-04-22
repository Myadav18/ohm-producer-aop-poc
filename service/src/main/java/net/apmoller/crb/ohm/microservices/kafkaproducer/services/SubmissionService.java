package net.apmoller.crb.ohm.microservices.kafkaproducer.services;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;

/**
 * This is interface to submission service This contains functionality to submit a request by publishing it to Kafka and
 * storing the message acknowledgement in Cassandra
 */
public interface SubmissionService {

    User submit(String name, String dept);
}
