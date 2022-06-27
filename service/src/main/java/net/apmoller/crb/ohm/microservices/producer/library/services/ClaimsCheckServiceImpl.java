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
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class ClaimsCheckServiceImpl<T> implements ClaimsCheckService<T> {

    @Autowired
    private FileService fileService;

    @Autowired
    private MessagePublisherUtil<T> messagePublisherUtil;

    @Value(ConfigConstants.AZURE_STORAGE_CONTAINER_NAME)
    private String CONTAINER_NAME;

    @Value(ConfigConstants.BLOB_ITEM_NAME_PREFIX)
    private String BLOB_ITEM_NAME_PREFIX;


    @Override
    public void handleClaimsCheckAfterGettingMemoryIssue(Map<String, Object> kafkaHeader, String producerTopic, T data)
            throws ClaimsCheckFailedException {
        try {
            long time = System.currentTimeMillis();
            String url = uploadToAzureBlob(
                    CompressionUtil.gzipCompress(data.toString().getBytes(StandardCharsets.UTF_8)));
            var claimscheckpayload = ClaimsCheckRequestPayload.newBuilder().setClaimsCheckBlobUrl(url).build();
            log.info("time taken to upload file to azure blob {} ms", System.currentTimeMillis() - time);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<String, T>(producerTopic, (T) claimscheckpayload);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Published to Kafka topic post claim check in {} ms", System.currentTimeMillis() - time);
        } catch (Exception e) {
            log.error("error occured while uploading to azure blob", e);
            throw new ClaimsCheckFailedException("Claims check failed while doing upload to blob", e);
        }
    }

    public String uploadToAzureBlob(byte[] compressedPayload) throws ClaimsCheckFailedException {
        try {
            return fileService.uploadFile(compressedPayload, CONTAINER_NAME, BLOB_ITEM_NAME_PREFIX + UUID.randomUUID()
                    + "_" + TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        } catch (Exception e) {
            log.error("error occured while uploading to azure blob", e);
            throw new ClaimsCheckFailedException("Claims check failed while doing upload to blob", e);
        }
    }
}
