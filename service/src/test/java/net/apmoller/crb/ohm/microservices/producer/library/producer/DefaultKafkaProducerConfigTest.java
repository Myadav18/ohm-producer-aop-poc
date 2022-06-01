package net.apmoller.crb.ohm.microservices.producer.library.producer;

import org.apache.kafka.common.serialization.StringSerializer;
import org.mockito.Mock;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@DisplayName("Kafka Config Test")
public class DefaultKafkaProducerConfigTest {

    private DefaultKafkaProducerConfig kafkaConfig;
    @Mock
    private DefaultKafkaProducerFactory defaultKafkaProducerFactory;

    @BeforeEach
    public void setUp() {
        kafkaConfig = new DefaultKafkaProducerConfig();

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
        ProducerFactory<String, Object> producerFactory = kafkaConfig.producerFactory();
        assertNotNull(producerFactory, "UserProducerFactory should not be null");
    }

    @Test
    @DisplayName("User Producer Factory for AvroSerilaizer")
    public void userProducerFactoryForAvro() {
        ReflectionTestUtils.setField(kafkaConfig, "avroEnabled", true);
        ReflectionTestUtils.setField(kafkaConfig, "schemaRegistryUrl", "url");
        ReflectionTestUtils.setField(kafkaConfig, "schemaRegistryUserInfo", "IABC:vk");
        ProducerFactory<String, Object> producerFactory = kafkaConfig.producerFactory();
        assertNotNull(producerFactory, "UserProducerFactory should not be null");
    }

    @Test
    @DisplayName("User Producer Factory for AvroSerilaizer")
    public void userProducerFactoryWithoutAvro() {
        ReflectionTestUtils.setField(kafkaConfig, "avroEnabled", false);
        ProducerFactory<String, Object> producerFactory = kafkaConfig.producerFactory();
        assertNotNull(producerFactory, "UserProducerFactory should not be null");
        assertEquals(StringSerializer.class,producerFactory.getConfigurationProperties().get(VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    @DisplayName("Kafka Admin")
    public void kafkaAdmin() {
        KafkaAdmin kafkaAdmin = kafkaConfig.kafkaAdmin();
        assertNotNull(kafkaAdmin, "KafkaAdmin should not be null");
    }
}
