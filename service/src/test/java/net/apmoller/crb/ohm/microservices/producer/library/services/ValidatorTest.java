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
public class ValidatorTest {

    @Autowired
    private ConfigValidator Validator;

    @MockBean
    private ApplicationContext context;

    @Test
    void testInvalidTopic() {
        String producerTopic = "";
        String bootstrapServer = "localhost:9092";
        try {
            Validator.checkValidation(producerTopic, bootstrapServer);
        } catch (InvalidTopicException ex) {
            log.info("Topic name is not valid , it can't be null or Empty");
        }
    }

    @Test
    void testInvalidTopicPlaceholder() {
        String producerTopic = "${";
        String bootstrapServer = "localhost:9092";
        try {
            Validator.checkValidation(producerTopic, bootstrapServer);
        } catch (InvalidTopicException ex) {
            log.info("Topic Placeholder is not valid");
        }
    }

    @Test
    void testInvalidServerPlaceholder() {
        String producerTopic = "test";
        String bootstrapServer = "${";
        try {
            Validator.checkValidation(producerTopic, bootstrapServer);
        } catch (KafkaServerNotFoundException ex) {
            log.info("Placeholder for Bootstrap Server is not Correct");
        }
    }

    @Test
    void testInvalidServerConfig() {
        String producerTopic = "test";
        String bootstrapServer = "";
        try {
            Validator.checkValidation(producerTopic, bootstrapServer);
        } catch (KafkaServerNotFoundException ex) {
            log.info("Bootstrap Server is not able to Connected, it can't be Empty or null");
        }
    }

    @Test
    void testSuccessCase() {
        String producerTopic = "test";
        String bootstrapServer = "localhost:9092";
        Validator.checkValidation(producerTopic, bootstrapServer);
    }

}
