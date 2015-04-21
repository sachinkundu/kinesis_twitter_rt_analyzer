import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;

public class DynamoDBConnectorExecutor extends KinesisConnectorExecutorBase<KinesisMessageModel, Map<String, AttributeValue>> {

    private static final Log LOG = LogFactory.getLog(DynamoDBConnectorExecutor.class);

    // Create AWS Resource constants
    private static final String CREATE_DYNAMODB_DATA_TABLE = "createDynamoDBDataTable";
    private static final boolean DEFAULT_CREATE_RESOURCES = false;

    // Create Amazon DynamoDB Resource constants
    private static final String DYNAMODB_KEY = "dynamoDBKey";
    private static final String DYNAMODB_READ_CAPACITY_UNITS = "readCapacityUnits";
    private static final String DYNAMODB_WRITE_CAPACITY_UNITS = "writeCapacityUnits";
    private static final Long DEFAULT_DYNAMODB_READ_CAPACITY_UNITS = 1l;
    private static final Long DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS = 1l;

    // Class variables
    protected final KinesisConnectorConfiguration config;
    private final Properties properties;

    public DynamoDBConnectorExecutor(String configFile) {
        InputStream configStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);

        if (configStream == null) {
            String msg = "Could not find resource " + configFile + " in the classpath";
            throw new IllegalStateException(msg);
        }
        properties = new Properties();
        try {
            properties.load(configStream);
            configStream.close();
        } catch (IOException e) {
            String msg = "Could not load properties file " + configFile + " from classpath";
            throw new IllegalStateException(msg, e);
        }
        this.config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider());
        setupAWSResources();

        // Initialize executor with configurations
        super.initialize(config);
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>>
    getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<KinesisMessageModel, Map<String, AttributeValue>>(new DynamoDBMessageModelPipeline(),
                config);
    }
    /**
     * Returns an {@link AWSCredentialsProvider} with the permissions necessary to accomplish all specified
     * tasks. At the minimum it will require read permissions for Amazon Kinesis. Additional read permissions
     * and write permissions may be required based on the Pipeline used.
     *
     * @return
     */
    public AWSCredentialsProvider getAWSCredentialsProvider() {
        return new DefaultAWSCredentialsProviderChain();
    }

    /**
     * Setup necessary AWS resources for the samples. By default, the Executor does not create any
     * AWS resources. The user must specify true for the specific create properties in the
     * configuration file.
     */
    private void setupAWSResources() {
        if (parseBoolean(CREATE_DYNAMODB_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
            String key = properties.getProperty(DYNAMODB_KEY);
            Long readCapacityUnits =
                    parseLong(DYNAMODB_READ_CAPACITY_UNITS, DEFAULT_DYNAMODB_READ_CAPACITY_UNITS, properties);
            Long writeCapacityUnits =
                    parseLong(DYNAMODB_WRITE_CAPACITY_UNITS, DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS, properties);
            createDynamoDBTable(key, readCapacityUnits, writeCapacityUnits);
        }
    }

    private void createDynamoDBTable(String key, long readCapacityUnits, long writeCapacityUnits) {
        LOG.info("Creating Amazon DynamoDB table " + config.DYNAMODB_DATA_TABLE_NAME);
        AmazonDynamoDBClient dynamodbClient = new AmazonDynamoDBClient(config.AWS_CREDENTIALS_PROVIDER);
        dynamodbClient.setEndpoint(config.DYNAMODB_ENDPOINT);
        DynamoDBUtils.createTable(dynamodbClient,
                config.DYNAMODB_DATA_TABLE_NAME,
                key,
                readCapacityUnits,
                writeCapacityUnits);
    }

    /**
     * Helper method used to parse boolean properties.
     *
     * @param property
     *        The String key for the property
     * @param defaultValue
     *        The default value for the boolean property
     * @param properties
     *        The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static boolean parseBoolean(String property, boolean defaultValue, Properties properties) {
        return Boolean.parseBoolean(properties.getProperty(property, Boolean.toString(defaultValue)));
    }

    /**
     * Helper method used to parse long properties.
     *
     * @param property
     *        The String key for the property
     * @param defaultValue
     *        The default value for the long property
     * @param properties
     *        The properties file to get property from
     * @return property from property file, or if it is not specified, the default value
     */
    private static long parseLong(String property, long defaultValue, Properties properties) {
        return Long.parseLong(properties.getProperty(property, Long.toString(defaultValue)));
    }

    public static void main(String[] args) {
        String configFile = "DynamoDB.properties";
        DynamoDBConnectorExecutor dynamoDBExecutor = new DynamoDBConnectorExecutor(configFile);
        dynamoDBExecutor.run();
    }
}