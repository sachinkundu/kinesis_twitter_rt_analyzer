import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sends HTTP referrer pairs to Amazon Kinesis.
 */
public class KinesisProducer {
    private static final Log LOG = LogFactory.getLog(KinesisProducer.class);

    private AmazonKinesis kinesis;
    private String streamName;

    private final ObjectMapper JSON = new ObjectMapper();

    public KinesisProducer(AmazonKinesis kinesis, String streamName) {
        if (kinesis == null) {
            throw new IllegalArgumentException("kinesis must not be null");
        }
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("streamName must not be null or empty");
        }
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    public void sendTweet(String tweet, String partitionKey) {
        byte[] bytes;
        try {
            bytes = JSON.writeValueAsBytes(tweet);
        } catch (IOException e) {
            LOG.warn("Skipping pair. Unable to serialize: '" + tweet + "'", e);
            return;
        }

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        putRecord.setPartitionKey(partitionKey);
        putRecord.setData(ByteBuffer.wrap(bytes));
        // Order is not important for this application so we do not send a SequenceNumberForOrdering
        putRecord.setSequenceNumberForOrdering(null);

        try {
            kinesis.putRecord(putRecord);
        } catch (ProvisionedThroughputExceededException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Thread %s's Throughput exceeded. Waiting 10ms", Thread.currentThread().getName()));
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}