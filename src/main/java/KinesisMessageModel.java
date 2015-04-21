import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * This is the data model for the Tweets
 *
 */
public class KinesisMessageModel implements Serializable {

    public String str_id;
    public Integer favorite_count;
//    public Float coordinates_x;
//    public Float coordinates_y;
    public Integer retweet_count;
    public String text;
    public String source;

    /**
     * Default constructor for Jackson JSON mapper - uses bean pattern.
     */
    public KinesisMessageModel() {

    }

    public KinesisMessageModel(String str_id,
                               Integer favorite_count,
                               Float coordinates_x,
                               Float coordinates_y,
                               Integer retweet_count,
                               String text,
                               String source)
    {
        this.str_id = str_id;
        this.favorite_count = favorite_count;
//        this.coordinates_x = coordinates_x;
//        this.coordinates_y = coordinates_y;
        this.retweet_count = retweet_count;
        this.text = text;
        this.source = source;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return super.toString();
        }
    }

    public String getStr_id()  { return str_id; }
    public void setStr_id(String str_id) {
        this.str_id = str_id;
    }

    public Integer getFavoriteCount() {
        return favorite_count;
    }
    public void setFavoriteCount(Integer count) {
        this.favorite_count = count;
    }

//    public Float getCoordinateX() {
//        return coordinates_x;
//    }
//    public void setCoordinateX(Float x) {
//        this.coordinates_x= x;
//    }
//
//    public Float getCoordinateY() { return coordinates_y; }
//    public void  setCoordinateY(Float y) { this.coordinates_y= y; }

    public Integer getRetweetCount() {
        return retweet_count;
    }
    public void setRetweetCount(Integer count) { this.retweet_count = count; }

    public String getSource()  { return source; }
    public void setSource(String source) { this.source = source; }

    public String getText()  { return text; }
    public void setText(String text) { this.text = text; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + str_id.hashCode();
        result = prime * result + ((favorite_count == null) ? 0 : favorite_count.hashCode());
//        result = prime * result + ((coordinates_x == null) ? 0 : coordinates_x.hashCode());
//        result = prime * result + ((coordinates_y == null) ? 0 : coordinates_y.hashCode());
        result = prime * result + (retweet_count == 0 ? 0 : retweet_count.hashCode());
        result = prime * result + (text == null ? 1231 : text.hashCode());
        result = prime * result + (source == null ? 1231 : source.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof KinesisMessageModel)) {
            return false;
        }
        KinesisMessageModel other = (KinesisMessageModel) obj;
        if (str_id == null) {
            if (other.str_id != null) {
                return false;
            }
        } else if (!str_id.equals(other.str_id)) {
            return false;
        }
        if (favorite_count == null) {
            if (other.favorite_count != null) {
                return false;
            }
        } else if (!favorite_count.equals(other.favorite_count)) {
            return false;
        }
//        if (coordinates_x == null) {
//            if (other.coordinates_x != null) {
//                return false;
//            }
//        } else if (!coordinates_x.equals(other.coordinates_x)) {
//            return false;
//        }
//        if (coordinates_y == null) {
//            if (other.coordinates_y != null) {
//                return false;
//            }
//        } else if (!coordinates_y.equals(other.coordinates_y)) {
//            return false;
//        }
        if (retweet_count != other.retweet_count) {
            return false;
        }
        if (text != other.text) {
            return false;
        }
        if (source != other.source) {
            return false;
        }
        return true;
    }
}