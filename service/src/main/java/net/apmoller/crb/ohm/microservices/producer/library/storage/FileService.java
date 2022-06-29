package net.apmoller.crb.ohm.microservices.producer.library.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;

@Log4j2
@Service
public class FileService {
    private final BlobServiceClient blobServiceClient;

    @Autowired
    public FileService(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    public String uploadFile(@NonNull byte[] file, String containerName, String filename) throws IOException {
        String uploadedUrl = null;
        BlobContainerClient blobContainerClient = getBlobContainerClient(containerName);
        String filenamefinal = filename.concat(".dat");
        BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(filenamefinal).getBlockBlobClient();
        try (BlobOutputStream bos = blockBlobClient.getBlobOutputStream()) {
            byte[] byteArray = file;
            bos.write(byteArray);
        }
        String url = blockBlobClient.getBlobUrl();
        log.info("blob url {} ", url);
        return url;
    }

    private @NonNull BlobContainerClient getBlobContainerClient(@NonNull String containerName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!blobContainerClient.exists()) {
            blobContainerClient.create();
        }
        return blobContainerClient;
    }
}