package ch.cern.exdemon.http;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;

import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

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

    public PostMethod toPostMethod() throws UnsupportedEncodingException {
        StringRequestEntity requestEntity = new StringRequestEntity(json.toString(), "application/json", "UTF-8");
        
        PostMethod postMethod = new PostMethod(url);
        postMethod.setRequestEntity(requestEntity);
        
        return postMethod;
    }

}
