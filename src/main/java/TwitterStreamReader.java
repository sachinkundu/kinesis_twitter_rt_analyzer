import org.json.*;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamReader {

    public static void run(String consumerKey, String consumerSecret, String token,
                           String secret, KinesisProducer producer) throws InterruptedException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("#CWC15"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();
        while (true) {
            String msg = queue.take();
            JSONObject obj = new JSONObject(msg);
            producer.sendTweet(msg, obj.getString("id_str"));
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: ");
                break;
            }
        }

        client.stop();
        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }
}