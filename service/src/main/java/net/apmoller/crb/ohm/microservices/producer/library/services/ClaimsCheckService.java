package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;

import java.util.Map;

public interface ClaimsCheckService<T> {
    void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, String s, T message)
            throws ClaimsCheckFailedException;
}