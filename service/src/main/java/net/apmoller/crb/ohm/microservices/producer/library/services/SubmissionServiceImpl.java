package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.models.User;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.producer.KafkaMessageGenerator;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * This is a implementation class for {@link SubmissionService} interface.
 */
@Service
@Slf4j
public class SubmissionServiceImpl implements SubmissionService {

    private final KafkaMessageGenerator kafkaMessageGenerator;

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final String kafkaproducerCommandTopic;

    @Value("${spring.application.name}")
    private String serviceName;

    public SubmissionServiceImpl(KafkaMessageGenerator kafkaMessageGenerator,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.command.submission.topic}") String kafkaproducerCommandTopic) {
        this.kafkaMessageGenerator = kafkaMessageGenerator;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaproducerCommandTopic = kafkaproducerCommandTopic;
    }

    @Override
    public User submit(String name, String dept) {

        UUID commandCorrelationId = UUID.randomUUID();
        log.debug("Going to send submission message for command correlation Id {}", commandCorrelationId);

        User user = new User(name, dept);

        String kafkaMessage = kafkaMessageGenerator.getMessage(user);

        try {
            RecordMetadata recordMetadata = kafkaTemplate.send(
                    new ProducerRecord<>(kafkaproducerCommandTopic, commandCorrelationId.toString(), kafkaMessage))
                    .get().getRecordMetadata();
            log.info(
                    "Command correlation Id: {} - Published to Kafka topic {} on partition {} at offset {}. Submission message: {}",
                    commandCorrelationId, kafkaproducerCommandTopic, recordMetadata.partition(),
                    recordMetadata.offset(), kafkaMessage);
        } catch (InterruptedException e) {
            log.error("InterruptedException occurred! {}", e);
            throw new InternalServerException(e.getMessage());
        } catch (ExecutionException e) {
            log.error("ExecutionException occurred! {}", e);
            throw new InternalServerException(e.getMessage());
        }
        return User.builder().name(name).dept(dept).build();
    }
}
