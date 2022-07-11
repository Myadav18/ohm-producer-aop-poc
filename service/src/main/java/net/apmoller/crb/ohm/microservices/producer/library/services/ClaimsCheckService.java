package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;

import java.io.IOException;
import java.util.Map;

public interface ClaimsCheckService<T> {
    void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, T message)
            throws ClaimsCheckFailedException, IOException;

    void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, Map<String, String> topics,
            T message) throws ClaimsCheckFailedException, IOException;
}