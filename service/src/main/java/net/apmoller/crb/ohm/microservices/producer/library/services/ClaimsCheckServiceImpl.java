package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.claimscheck.request.ClaimsCheckRequestPayload;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.DLTException;
import net.apmoller.crb.ohm.microservices.producer.library.storage.FileService;
import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class ClaimsCheckServiceImpl<T> implements ClaimsCheckService<T> {

    @Autowired
    private FileService fileService;

    @Autowired
    private MessagePublisherUtil<T> messagePublisherUtil;

    @Autowired
    private ConfigValidator<T> configValidator;

    @Autowired
    private ApplicationContext context;

    @Value(ConfigConstants.AZURE_STORAGE_CONTAINER_NAME)
    private String containerName;

    @Value(ConfigConstants.BLOB_ITEM_NAME_PREFIX)
    private String blobItemNamePrefix;

    private static final String BLOB_UPLOAD_ERROR_MESSAGE = "Error occurred while uploading Payload with Correlation-Id {} to azure blob";

    private String correlationId;

    /*
     * Method to handle upload to Azure blob storage and posting storage url on claims check topic
     */
    @Override
    public void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, Map<String, String> topics,
                                                         T message) throws ClaimsCheckFailedException, DLTException {
        ProducerRecord<String, T> producerRecord;
        ClaimsCheckRequestPayload claimsCheckPayload = null;
        correlationId = configValidator.getCorrelationId(kafkaHeader);
        if (configValidator.claimsCheckTopicNotPresent(topics))
            throw new ClaimsCheckFailedException("Claims check topic not found");
        try {
            long time = System.currentTimeMillis();
            String url = uploadToAzureBlob(CompressionUtil.gzipCompress(message));
            claimsCheckPayload = ClaimsCheckRequestPayload.newBuilder().setClaimsCheckBlobUrl(url).build();
            log.info("Time taken to upload file with Correlation-Id {} to azure blob {} ms", correlationId, System.currentTimeMillis() - time);
            producerRecord = new ProducerRecord<>(topics.get(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY), (T) claimsCheckPayload);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Published message with Correlation-Id {} to Kafka topic post claim check in {} ms", correlationId, System.currentTimeMillis() - time);
        } catch (ClaimsCheckFailedException ex) {
            log.error(BLOB_UPLOAD_ERROR_MESSAGE, correlationId, ex);
            throw ex;
        } catch (Exception e) {
            log.error("Exception while posting Payload with Correlation-Id {} to claims check topic ", correlationId, e);
            // Send to DLT
            if (configValidator.claimsCheckDltPresent(topics)) {
                producerRecord = new ProducerRecord<>(topics.get(ConfigConstants.CLAIMS_CHECK_DLT_KEY), (T) claimsCheckPayload);
                messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
                throw new DLTException(String.format("Successfully published Payload with Correlation-Id %s to Claims check DLT", correlationId));
            } else {
                log.info("Claims check DLT for Correlation-Id {} not added in input topic map hence throwing exception", correlationId);
                throw new ClaimsCheckFailedException("Dead letter topic not found");
            }
        }
    }

    /*
     * Method to upload blob to Azure storage
     */
    public String uploadToAzureBlob(byte[] compressedPayload) throws ClaimsCheckFailedException {
        try {
            return fileService.uploadFile(compressedPayload, containerName, blobItemNamePrefix + UUID.randomUUID() + "_"
                    + TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        } catch (Exception e) {
            log.error(BLOB_UPLOAD_ERROR_MESSAGE, correlationId, e);
            throw new ClaimsCheckFailedException("Claims check failed while uploading to blob", e);
        }
    }
}
