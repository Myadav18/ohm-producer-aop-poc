package net.apmoller.crb.ohm.microservices.producer.library.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Metrics;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class contains all the configuration information for Kafka producer factory to be able to create a Kafka
 * template for publishing messages
 */
@Setter
@Getter
@Slf4j
@RequiredArgsConstructor
public class DefaultKafkaProducerConfig {

    @Value("${kafka.bootstrapserver}")
    private String bootstrapServers;
    @Value("${kafka.producer.login-module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String loginModule;
    @Value("${kafka.properties.sasl.mechanism:PLAIN}")
    private String saslMechanism;
    @Value("${kafka.properties.ssl.truststore.location:}")
    private String truststoreLocation;
    @Value("${kafka.properties.ssl.truststore.password:}")
    private String truststorePassword;
    @Value("${kafka.producer.acks-config:all}")
    private String producerAcksConfig;
    @Value("${kafka.producer.linger:1}")
    private int producerLinger;
    @Value("${kafka.producer.timeout:30000}")
    private int producerRequestTimeout;
    @Value("${kafka.producer.batch-size:16384}")
    private int producerBatchSize;
    @Value("${kafka.producer.send-buffer:131072}")
    private int producerSendBuffer;
    @Value("${kafka.properties.security.protocol:SASL_SSL}")
    private String securityProtocol;
    @Value("${kafka.properties.schema.registry.url:}")
    private String schemaRegistryUrl;
    @Value("${kafka.properties.schema.registry.basic.auth.user.info:}")
    private String schemaRegistryUserInfo;
    @Value("${kafka.properties.basic.auth.credentials.source: USER_INFO}")
    private String schemaRegistryAuth;
    @Value("${kafka.producer.value-serializer}")
    private String valueSerializer;

    private void addSaslProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol,
            String loginModule) {
        log.info("Creating SASL Properties, saslMechanism:{}, securityProtocol:{}", saslMechanism, securityProtocol);
        properties.put("security.protocol", securityProtocol);
        properties.put("sasl.mechanism", saslMechanism);
        properties.put("sasl.jaas.config", loginModule);
    }

    private static void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
        if (Objects.nonNull(location) && !location.isEmpty() && Objects.nonNull(password) && !password.isEmpty()) {
            properties.put("ssl.truststore.location", location);
            properties.put("ssl.truststore.password", password);
        }
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        log.info("Default Kafka Config");
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeout);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, schemaRegistryAuth);
        properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, schemaRegistryUserInfo);

        addSaslProperties(properties, saslMechanism, securityProtocol, loginModule);
        addTruststoreProperties(properties, truststoreLocation, truststorePassword);
        DefaultKafkaProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(properties);
        producerFactory.addListener(new MicrometerProducerListener<>(Metrics.globalRegistry,
                Collections.singletonList(new ImmutableTag("customTag", "customTagValue"))));

        return producerFactory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
