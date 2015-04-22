import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import com.amazonaws.services.kinesis.model.Record;
import org.json.JSONObject;

public abstract class JsonTransformer<T, U> implements ITransformer<T, U> {
    private static final Log LOG = LogFactory.getLog(JsonTransformer.class);
    protected Class<T> inputClass;
    private Gson gson;
    private final Constructor<? extends T> ctor;
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
    private T field;

    public JsonTransformer(Class<T> inputClass) throws NoSuchMethodException {
        this.inputClass = inputClass;
        this.ctor = inputClass.getConstructor();
        gson = new Gson();
    }

    private T mapTweet(T tweet, JSONObject json) {
        ((KinesisMessageModel)(tweet)).str_id = json.getString("id_str");
        ((KinesisMessageModel)(tweet)).retweet_count = json.getInt("retweet_count");
        ((KinesisMessageModel)(tweet)).favorite_count= json.getInt("favorite_count");
        ((KinesisMessageModel)(tweet)).text = json.getString("text");
        ((KinesisMessageModel)(tweet)).source = json.getString("source");
        Object coordinates = json.get("coordinates");
        if (!coordinates.equals(null) ) {
            ((KinesisMessageModel)(tweet)).coordinates_x = (Double)(((JSONObject)(coordinates))
                                                                    .getJSONArray("coordinates")
                                                                    .get(0));
            ((KinesisMessageModel)(tweet)).coordinates_y = (Double)(((JSONObject)(coordinates))
                                                                    .getJSONArray("coordinates")
                                                                    .get(1));
        } else {
            ((KinesisMessageModel)(tweet)).coordinates_x = null;
            ((KinesisMessageModel)(tweet)).coordinates_y = null;
        }
        return tweet;
    }

    public void makeModelInstance() throws Exception {
        field = ctor.newInstance();
    }

    @Override
    public T toClass(Record record) throws IOException {
        try {
            String data = decoder.decode(record.getData()).toString();
            String jsonData = gson.fromJson(data, String.class);
            JSONObject obj = new JSONObject(jsonData);
            try {
                makeModelInstance();
            } catch (Exception e){
                LOG.error(e.toString());
            }
            return mapTweet(field, obj);
        } catch (IOException e) {
            String message = "Error parsing record from JSON: " + new String(record.getData().array());
            LOG.error(message, e);
            throw new IOException(message, e);
        }
    }

}