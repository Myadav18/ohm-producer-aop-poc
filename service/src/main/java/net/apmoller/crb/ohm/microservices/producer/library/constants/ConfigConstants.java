package net.apmoller.crb.ohm.microservices.producer.library.constants;

public class ConfigConstants {

    private ConfigConstants() {
        // Private constructor
    }

    public static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    public static final String BOOTSTRAP_SERVER = "${kafka.bootstrapserver}";
    public static final String RETRY_TOPIC = "${kafka.notification.retry-topic}";
    public static final String DLT = "${kafka.notification.dead-letter-topic}";
    public static final String NOTIFICATION_TOPIC_KEY = "notification-topic";
    public static final String RETRY_TOPIC_KEY = "retry-topic";
    public static final String DEAD_LETTER_TOPIC_KEY = "dead-letter-topic";
    public static final String SCRAM_SASL_MECHANISM = "SCRAM-SHA-256";

    // Error messages constants
    public static final String INVALID_PAYLOAD_ERROR_MSG = "Payload can't be Empty or null";
    public static final String INVALID_TOPIC_MAP_ERROR_MSG = "Map containing topic names cannot be null or Empty";
    public static final String INVALID_NOTIFICATION_TOPIC_ERROR_MSG = "Notification topic name cannot be null or Empty";
    public static final String INVALID_RETRY_TOPIC_ERROR_MSG = "Retry topic name cannot be null or Empty";
    public static final String INVALID_DLT_ERROR_MSG = "Dead letter topic name cannot be null or Empty";
    public static final String INVALID_BOOTSTRAP_SERVER_ERROR_MSG = "Bootstrap server name cannot be null or empty";

    public static final String INVALID_NOTIFICATION_TOPIC_PLACEHOLDER = "Placeholder for Main_topic is not valid";
    public static final String INVALID_RETRY_TOPIC_PLACEHOLDER = "Placeholder for Retry_topic is not valid";
    public static final String INVALID_DLT_TOPIC_PLACEHOLDER = "Placeholder for Dlt_Topic is not valid";
    public static final String INVALID_BOOTSTRAP_PLACEHOLDER = "Placeholder for Bootstrap Server is not Correct";

}
