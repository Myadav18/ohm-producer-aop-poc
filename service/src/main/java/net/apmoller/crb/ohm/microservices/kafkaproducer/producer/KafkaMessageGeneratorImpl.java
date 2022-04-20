package net.apmoller.crb.ohm.microservices.kafkaproducer.producer;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.kafkaproducer.exceptions.*;
import org.springframework.stereotype.Service;

/**
 * This is a implementation class for {@link KafkaMessageGenerator} interface.
 */
@Service
@AllArgsConstructor
@Slf4j
public class KafkaMessageGeneratorImpl implements KafkaMessageGenerator {

    private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();

    @Override
    public String getMessage(User user) {
        try {
            return OBJECT_WRITER.writeValueAsString(user);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException occurred! {}", e);
            throw new InternalServerException(e.getMessage());
        }
    }
}
