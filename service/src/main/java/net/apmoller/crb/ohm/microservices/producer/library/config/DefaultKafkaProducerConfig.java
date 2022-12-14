package net.apmoller.crb.ohm.microservices.producer.library.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.micrometer.core.aop.CountedAspect;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;

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
public class DefaultKafkaProducerConfig<T> {

    @Value("${kafka.bootstrapserver}")
    private String bootstrapServers;
    @Value("${kafka.producer.login-module:}")
    private String loginModule;
    @Value("${kafka.properties.sasl.mechanism:PLAIN}")
    private String saslMechanism;
    @Value("${kafka.producer.acks-config:all}")
    private String producerAcksConfig;
    @Value("${kafka.producer.linger:5}")
    private int producerLinger;
    @Value("${kafka.producer.batch-size:16384}")
    private int producerBatchSize;
    @Value("${kafka.producer.send-buffer:131072}")
    private int producerSendBuffer;
    @Value("${kafka.producer.request.timeout.ms:30000}")
    private int producerRequestTimeoutMs;
    @Value("${kafka.producer.retry.backoff.ms:500}")
    private int retryBackoffMs;
    @Value("${kafka.properties.security.protocol:SASL_SSL}")
    private String securityProtocol;
    @Value("${kafka.properties.schema.registry.url:}")
    private String schemaRegistryUrl;
    @Value("${kafka.properties.schema.registry.ssl.protocol:}")
    private String schemaRegistrySslProtocol;
    @Value("${kafka.properties.schema.sasl.mechanism:}")
    private String schemaSaslMechanism;
    @Value("${kafka.properties.schema.registry.basic.auth.user.info:}")
    private String schemaRegistryUserInfo;
    @Value("${kafka.properties.basic.auth.credentials.source: USER_INFO}")
    private String schemaRegistryAuth;
    @Value("${kafka.properties.schema.registry.ssl.truststore.location:}")
    private String truststoreLocation;
    @Value("${kafka.properties.schema.registry.ssl.truststore.password:}")
    private String truststorePassword;
    @Value("${kafka.properties.schema.registry.ssl.keystore.location:}")
    private String keystoreLocation;
    @Value("${kafka.properties.schema.registry.ssl.keystore.password:}")
    private String keystorePassword;
    @Value("${kafka.properties.schema.registry.ssl.key.password:}")
    private String keyPassword;
    @Value("${kafka.properties.use.latest.version:true}")
    private boolean useLatestSchemaVersion;
    @Value("${kafka.properties.specific.avro.reader:true}")
    private boolean specificAvroReader;
    @Value("${kafka.properties.auto.register.schemas:false}")
    private boolean autoRegisterSchemas;
    @Value("${kafka.properties.ssl.enabled.protocols:}")
    private String sslEnabledProtocol;
    @Value("${kafka.properties.ssl.endpoint.identification.algorithm:https}")
    private String sslAlgorithm;
    @Value("${kafka.properties.ssl.protocol:TLS}")
    private String sslProtocol;
    @Value("${kafka.producer.compression.type:gzip}")
    private String compressionType;
    @Value("${kafka.producer.max.request.size:15000000}")
    private int maxRequestSize;
    @Value("${kafka.properties.saslRequired:true}")
    private String saslRequired;

    @Bean
    public ProducerFactory<String, T> producerFactoryForAvro() {
        log.info("Default Kafka Config For Avro");
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeoutMs);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        addSchemaRegistryProperties(properties);
        addSecurityProperties(properties, saslMechanism, securityProtocol, loginModule);
        addTruststoreProperties(properties);
        DefaultKafkaProducerFactory<String, T> producerFactory = new DefaultKafkaProducerFactory<>(properties);
        producerFactory.addListener(new MicrometerProducerListener<>(Metrics.globalRegistry,
                Collections.singletonList(new ImmutableTag("customTag", "producer-library-metrics"))));

        return producerFactory;
    }

    @Bean
    public ProducerFactory<String, T> producerFactoryForJson() {
        log.info("Default Kafka Config");
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeoutMs);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        addSecurityProperties(properties, saslMechanism, securityProtocol, loginModule);
        addTruststoreProperties(properties);
        DefaultKafkaProducerFactory<String, T> producerFactory = new DefaultKafkaProducerFactory<>(properties);
        producerFactory.addListener(new MicrometerProducerListener<>(Metrics.globalRegistry,
                Collections.singletonList(new ImmutableTag("customTag", "producer-library-metrics"))));

        return producerFactory;
    }

    @Bean
    public KafkaTemplate<String, T> kafkaTemplateAvro() {
        return new KafkaTemplate<>(producerFactoryForAvro());
    }

    @Bean
    public KafkaTemplate<String, T> kafkaTemplateJson() {
        return new KafkaTemplate<>(producerFactoryForJson());
    }

    private void addSecurityProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol,
            String loginModule) {
        log.info("Creating SASL Properties, saslMechanism:{}, securityProtocol:{}, saslRequired:{}", saslMechanism,
                securityProtocol, saslRequired);
        if (Boolean.parseBoolean(saslRequired)) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            properties.put("sasl.jaas.config", loginModule);
        }
    }

    private void addSchemaRegistryProperties(Map<String, Object> properties) {
        if (Objects.nonNull(schemaRegistryUrl) && !schemaRegistryUrl.isEmpty()) {
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, schemaRegistryAuth);
            properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, schemaRegistryUserInfo);
            properties.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, useLatestSchemaVersion);
            properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
            properties.put("specific.avro.reader", specificAvroReader);
            if (ConfigConstants.SCRAM_SASL_MECHANISM.equalsIgnoreCase(schemaSaslMechanism)) {
                properties.put("schema.registry.ssl.protocol", schemaRegistrySslProtocol);
                properties.put("schema.sasl.mechanism", schemaSaslMechanism);
                properties.put("schema.registry.ssl.truststore.location", truststoreLocation);
                properties.put("schema.registry.ssl.truststore.password", truststorePassword);
                properties.put("schema.registry.ssl.keystore.location", keystoreLocation);
                properties.put("schema.registry.ssl.keystore.password", keystorePassword);
                properties.put("schema.registry.ssl.key.password", keystorePassword);
            }
        }
    }

    private void addTruststoreProperties(Map<String, Object> properties) {
        properties.put("ssl.protocol", sslProtocol);
        properties.put("ssl.enabled.protocols", sslEnabledProtocol);
        properties.put("ssl.endpoint.identification.algorithm", sslAlgorithm);
        if (ConfigConstants.SCRAM_SASL_MECHANISM.equalsIgnoreCase(schemaSaslMechanism)) {
            properties.put("ssl.truststore.location", truststoreLocation);
            properties.put("ssl.truststore.password", truststorePassword);
            properties.put("ssl.keystore.location", keystoreLocation);
            properties.put("ssl.keystore.password", keystorePassword);
            properties.put("ssl.key.password", keystorePassword);
        }
    }

    @Bean
    CountedAspect countedAspect(MeterRegistry registry) {
        return new CountedAspect(registry);
    }
}
