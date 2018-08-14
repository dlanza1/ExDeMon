package ch.cern.exdemon.http;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import ch.cern.exdemon.json.JSON;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class JsonPOSTRequest {

    @Getter
    private String url;

    @Getter
    private JSON json;
    
    public JsonPOSTRequest(String url, JSON json) {
        this.url = url;
        this.json = json;
    }

    public void addProperty(String key, String value) throws ParseException {
        json.setProperty(key, value);
    }

    public HttpPost toPostMethod() throws UnsupportedEncodingException {
        HttpPost httpPost = new HttpPost(url);
        
        httpPost.setEntity(new StringEntity(json.toString()));
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        
        return httpPost;
    }

    public JsonPOSTRequest setUrlIfNull(String url) {
        if(this.url == null)
            this.url = url;
        
        return this;
    }

}
