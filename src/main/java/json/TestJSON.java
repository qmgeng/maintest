package json;

import netease.ReaperUrl;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

/**
 * Created by qmgeng on 2015/8/11.
 */
public class TestJSON {

    public static String bean2Json(Object obj) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        StringWriter sw = new StringWriter();
        JsonGenerator gen = new JsonFactory().createJsonGenerator(sw);
        mapper.writeValue(gen, obj);
        gen.close();
        return sw.toString();
    }

    public static <T> T json2Bean(String jsonStr, Class<T> objClass) throws JsonParseException, JsonMappingException,
            IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonStr, objClass);
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        File fl = new File("C:\\netease.ReaperUrl.json");

        TypeReference<List<ReaperUrl>> listReaperUrl = new TypeReference<List<ReaperUrl>>() {
        };
        List<ReaperUrl> reaperUrls = (List<ReaperUrl>) mapper.readValue(fl, listReaperUrl);
        System.out.println(reaperUrls.size());
        System.out.println(Arrays.toString(reaperUrls.toArray()));

        for (ReaperUrl reaperUrl : reaperUrls) {
            System.out.println(reaperUrl.toString());
        }
    }
}
