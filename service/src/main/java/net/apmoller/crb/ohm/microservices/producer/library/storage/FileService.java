package net.apmoller.crb.ohm.microservices.producer.library.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.ByteArrayInputStream;

@Log4j2
@Service
public class FileService {
    private final BlobServiceClient blobServiceClient;

    @Autowired
    public FileService(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    public String uploadFile(@NonNull byte[] file, String containerName, String filename) {
        String uploadedUrl = null;
        BlobContainerClient blobContainerClient = getBlobContainerClient(containerName);

        BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(filename).getBlockBlobClient();
        try {
            BlockBlobItem block = blockBlobClient.upload(new ByteArrayInputStream(file), file.length);
            uploadedUrl = blockBlobClient.getBlobUrl();
        } catch (Exception e) {
            uploadedUrl = null;
            log.error("Error while uploading file to azure blob ", e);
        }
        return uploadedUrl;
    }

    private @NonNull BlobContainerClient getBlobContainerClient(@NonNull String containerName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!blobContainerClient.exists()) {
            blobContainerClient.create();
        }
        return blobContainerClient;
    }
}