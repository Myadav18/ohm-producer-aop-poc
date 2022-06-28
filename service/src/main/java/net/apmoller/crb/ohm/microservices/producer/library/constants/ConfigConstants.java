package net.apmoller.crb.ohm.microservices.producer.library.constants;

public class ConfigConstants {

    private ConfigConstants() {
        // Private constructor
    }

    public static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    public static final String BOOTSTRAP_SERVER = "${kafka.bootstrapserver}";
    public static final String RETRY_TOPIC = "${kafka.notification.retry-topic}";
    public static final String DLT = "${kafka.notification.dead-letter-topic}";
    public static final String CLAIMS_CHECK = "${kafka.notification.claimscheck-topic}";
    public static final String NOTIFICATION_TOPIC_KEY = "notification-topic";
    public static final String CLAIMS_CHECK_TOPIC_KEY = "claimscheck-topic";
    public static final String RETRY_TOPIC_KEY = "retry-topic";
    public static final String DEAD_LETTER_TOPIC_KEY = "dead-letter-topic";
    public static final String SCRAM_SASL_MECHANISM = "SCRAM-SHA-256";

    // Error messages constants
    public static final String INVALID_PAYLOAD_ERROR_MSG = "Payload can't be Empty or null";
    public static final String INVALID_TOPIC_MAP_ERROR_MSG = "Map containing topic names cannot be null or Empty";
    public static final String INVALID_NOTIFICATION_TOPIC_ERROR_MSG = "Notification topic name cannot be null or Empty";
    public static final String INVALID_BOOTSTRAP_SERVER_ERROR_MSG = "Bootstrap server name cannot be null or empty";
    public static final String INVALID_NOTIFICATION_TOPIC_PLACEHOLDER = "Placeholder for Main_topic is not valid";
    public static final String INVALID_BOOTSTRAP_PLACEHOLDER = "Placeholder for Bootstrap Server is not Correct";
    public static final String INVALID_KAFKA_HEADER_MAP_ERROR_MSG = "Kafka headers map cannot be null or empty";
    public static final String INVALID_KAFKA_HEADER_VALUE_ERROR_MSG = "Value for Kafka header: %s cannot be null or empty";

    // Azure constants
    public static final String BLOB_ITEM_NAME_PREFIX = "${events-payload.file-name}";
    public static final String AZURE_STORAGE_ACCOUNT_NAME = "${azure.storage.account-name}";
    public static final String AZURE_STORAGE_ACCOUNT_KEY = "${azure.storage.account-key}";
    public static final String AZURE_STORAGE_CONTAINER_NAME = "${azure.storage.container-name}";
    public static final String AZURE_STORAGE_ENDPOINT = "${azure.storage.endpoint}";
    public static final String AZURE_STORAGE_ENDPOINT_SUFFIX = "${azure.storage.endpoint-suffix}";
    public static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";
}
