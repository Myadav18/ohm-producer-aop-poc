package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest(classes = { ConfigValidator.class })
@ActiveProfiles({ "test" })
public class ConfigValidatorTest {

    @Autowired
    private ConfigValidator Validator;

    @MockBean
    private ApplicationContext context;

    @Test
    void testInvalidTopic() {
        String producerTopic = "";
        try {
            Validator.validateInputs(producerTopic);
        } catch (InvalidTopicException ex) {
            log.info("Topic name is not valid , it can't be null or Empty");
        }
    }

    @Test
    void testInvalidServerPlaceholder() {
        String producerTopic = "test";
        try {
            Validator.validateInputs(producerTopic);
        } catch (KafkaServerNotFoundException ex) {
            log.info("Placeholder for Bootstrap Server is not Correct");
        }
    }

    @Test
    void testInvalidServerConfig() {
        String producerTopic = "test";
        try {
            Validator.validateInputs(producerTopic);
        } catch (KafkaServerNotFoundException ex) {
            log.info("Bootstrap details cannot be empty, so unable to be connect");
        }
    }

}
