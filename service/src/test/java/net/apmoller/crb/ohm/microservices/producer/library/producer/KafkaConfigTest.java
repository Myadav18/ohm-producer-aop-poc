package net.apmoller.crb.ohm.microservices.producer.library.producer;

import net.apmoller.crb.ohm.microservices.producer.library.models.User;
import net.apmoller.crb.ohm.microservices.producer.library.producer.KafkaConfig;
import org.springframework.kafka.core.ProducerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@DisplayName("Kafka Config Test")
public class KafkaConfigTest {

    private KafkaConfig kafkaConfig;

    @MockBean
    private LocalValidatorFactoryBean validator;

    @BeforeEach
    public void setUp() {
        kafkaConfig = new KafkaConfig(validator);

        ReflectionTestUtils.setField(kafkaConfig, "bootstrapServers", "testServer:9092");
        ReflectionTestUtils.setField(kafkaConfig, "securityProtocol", "SASL_SSL");
        ReflectionTestUtils.setField(kafkaConfig, "saslMechanism", "PLAIN");
        ReflectionTestUtils.setField(kafkaConfig, "loginModule", "test");
        ReflectionTestUtils.setField(kafkaConfig, "username", "test");
        ReflectionTestUtils.setField(kafkaConfig, "password", "test");
        ReflectionTestUtils.setField(kafkaConfig, "truststoreLocation", "SRCLOCN");
        ReflectionTestUtils.setField(kafkaConfig, "truststorePassword", "SRCPWD");
        ReflectionTestUtils.setField(kafkaConfig, "kafkaClientId", "CONS");
        ReflectionTestUtils.setField(kafkaConfig, "producerAcksConfig", "all");
        ReflectionTestUtils.setField(kafkaConfig, "producerLinger", 1);
        ReflectionTestUtils.setField(kafkaConfig, "producerRequestTimeout", 30000);
        ReflectionTestUtils.setField(kafkaConfig, "producerBatchSize", 16384);
        ReflectionTestUtils.setField(kafkaConfig, "producerSendBuffer", 131072);
        ReflectionTestUtils.setField(kafkaConfig, "kafkaClientId", "rates-consumer");
    }

    @Test
    @DisplayName("Kafka Template")
    public void kafkaTemplate() {
        KafkaTemplate kafkaTemplate = kafkaConfig.kafkaTemplate();
        assertNotNull(kafkaTemplate, "KafkaTemplate should not be null");
    }

    @Test
    @DisplayName("User Producer Factory")
    public void userProducerFactory() {
        ProducerFactory<String, User> producerFactory = kafkaConfig.producerFactory();
        assertNotNull(producerFactory, "UserProducerFactory should not be null");
    }
}
