/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.kinesis.connectors.dynamodb.DynamoDBTransformer;

/**
 * A custom transfomer for {@link KinesisMessageModel} records in JSON format. The output is in a format
 * usable for insertions to Amazon DynamoDB.
 */
public class KinesisMessageModelDynamoDBTransformer extends
        JsonTransformer<KinesisMessageModel, Map<String, AttributeValue>> implements
        DynamoDBTransformer<KinesisMessageModel> {

    /**
     * Creates a new KinesisMessageModelDynamoDBTransformer.
     */
    public KinesisMessageModelDynamoDBTransformer() throws NoSuchMethodException {
        super(KinesisMessageModel.class);
    }

    @Override
    public Map<String, AttributeValue> fromClass(KinesisMessageModel message) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        putStringIfNonempty(item, "str_id", message.str_id);
        putIntegerIfNonempty(item, "favorite_count", message.favorite_count);
        putDoubleIfNonempty(item, "coordinates_x", message.coordinates_x);
        putDoubleIfNonempty(item, "coordinates_y", message.coordinates_y);
        putIntegerIfNonempty(item, "retweet_count", message.retweet_count);
        putStringIfNonempty(item, "text", message.text);
        putStringIfNonempty(item, "source", message.source);
        return item;
    }

    /**
     * Helper method to map nonempty String attributes to an AttributeValue.
     *
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to check before inserting into the item map
     */
    private void putStringIfNonempty(Map<String, AttributeValue> item, String key, String value) {
        if (!value.equals(null) && !value.isEmpty()) {
            item.put(key, new AttributeValue().withS(value));
        }
    }

    /**
     * Helper method to map nonempty Integer attributes to an AttributeValue.
     *
     * @param item The map of attribute names to AttributeValues to store the attribute in
     * @param key The key to store in the map
     * @param value The value to insert into the item map
     */
    private void putIntegerIfNonempty(Map<String, AttributeValue> item, String key, Integer value) {
        if(value != null)
            putStringIfNonempty(item, key, Integer.toString(value));
    }

    private void putDoubleIfNonempty(Map<String, AttributeValue> item, String key, Double value) {
        if(value != null)
            putStringIfNonempty(item, key, Double.toString(value));
    }}