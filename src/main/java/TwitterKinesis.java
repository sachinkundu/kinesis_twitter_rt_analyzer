import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class TwitterKinesis {
    public static void main(String[] args) {
        String streamName = "twitter-stream";
        Region region = RegionUtils.getRegion("us-east-1");
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        ClientConfiguration clientConfig = new ClientConfiguration();
        AmazonKinesis kinesis = new AmazonKinesisClient(credentialsProvider, clientConfig);
        kinesis.setRegion(region);
        StreamUtils streamUtils = new StreamUtils(kinesis);
        streamUtils.createStreamIfNotExists(streamName, 2);
        final KinesisProducer producer = new KinesisProducer(kinesis, streamName);
        try {
            TwitterStreamReader.run(args[0], args[1], args[2], args[3], producer);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}

