package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.claimscheck.request.ClaimsCheckRequestPayload;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.ClaimsCheckFailedException;
import net.apmoller.crb.ohm.microservices.producer.library.storage.FileService;
import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
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

    private static final String BLOB_UPLOAD_ERROR_MESSAGE = "Error occurred while uploading to azure blob";

    /*
     * Method to handle claims check for single producer flow
     */
    @Override
    public void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, T data)
            throws ClaimsCheckFailedException, IOException {
        ProducerRecord<String, T> producerRecord;
        ClaimsCheckRequestPayload claimsCheckPayload = null;
        String claimsCheckTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.CLAIMS_CHECK);
        if (configValidator.claimsCheckTopicNotPresent(claimsCheckTopic))
            throw new ClaimsCheckFailedException("claims check topic not found in config");
        try {
            long time = System.currentTimeMillis();
            String url = uploadToAzureBlob(CompressionUtil.gzipCompress(data));
            claimsCheckPayload = ClaimsCheckRequestPayload.newBuilder().setClaimsCheckBlobUrl(url).build();
            log.info("time taken to upload file to azure blob {} ms", System.currentTimeMillis() - time);
            producerRecord = new ProducerRecord<>(claimsCheckTopic, (T) claimsCheckPayload);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Published to Kafka topic post claim check in {} ms", System.currentTimeMillis() - time);
        } catch (ClaimsCheckFailedException ex) {
            log.error(BLOB_UPLOAD_ERROR_MESSAGE, ex);
            throw ex;
        } catch (Exception e) {
            log.error("Exception while posting claims check topic ", e);
            // Send to DLT
            var claimsCheckDlt = context.getEnvironment().resolvePlaceholders(ConfigConstants.CLAIMS_CHECK_DLT);
            if (configValidator.claimsCheckDltIsPresent(claimsCheckDlt)) {
                producerRecord = new ProducerRecord<>(claimsCheckDlt, (T) claimsCheckPayload);
                messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            } else {
                log.info("Claims check DLT not added in environment config");
                throw e;
            }
        }
    }

    /*
     * Method to handle claims check for multiple producer flow
     */
    @Override
    public void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, Map<String, String> topics,
            T message) throws ClaimsCheckFailedException, IOException {
        ProducerRecord<String, T> producerRecord;
        ClaimsCheckRequestPayload claimsCheckPayload = null;
        if (configValidator.claimsCheckTopicNotPresent(topics))
            throw new ClaimsCheckFailedException("Claims check topic not found");
        try {
            long time = System.currentTimeMillis();
            String url = uploadToAzureBlob(CompressionUtil.gzipCompress(message));
            claimsCheckPayload = ClaimsCheckRequestPayload.newBuilder().setClaimsCheckBlobUrl(url).build();
            log.info("time taken to upload file to azure blob {} ms", System.currentTimeMillis() - time);
            producerRecord = new ProducerRecord<>(topics.get(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY),
                    (T) claimsCheckPayload);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Published to Kafka topic post claim check in {} ms", System.currentTimeMillis() - time);
        } catch (ClaimsCheckFailedException ex) {
            log.error(BLOB_UPLOAD_ERROR_MESSAGE, ex);
            throw ex;
        } catch (Exception e) {
            log.error("Exception while posting claims check topic ", e);
            // Send to DLT
            if (configValidator.claimsCheckDltPresent(topics)) {
                producerRecord = new ProducerRecord<>(topics.get(ConfigConstants.CLAIMS_CHECK_DLT_KEY),
                        (T) claimsCheckPayload);
                messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            } else {
                log.info("Claims check DLT not added in input topic map hence throwing exception");
                throw e;
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
            log.error(BLOB_UPLOAD_ERROR_MESSAGE, e);
            throw new ClaimsCheckFailedException("Claims check failed while doing upload to blob", e);
        }
    }
}
