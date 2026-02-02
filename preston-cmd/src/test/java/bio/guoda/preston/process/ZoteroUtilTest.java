package bio.guoda.preston.process;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.List;

public class ZoteroUtilTest {

    @Test
    public void parseCreators() throws JsonProcessingException {
        String creatorsChunk = "[\n" +
                "            {\n" +
                "                \"creatorType\": \"author\",\n" +
                "                \"name\": \"FAO\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"creatorType\": \"editor\",\n" +
                "                \"firstName\": \"N\",\n" +
                "                \"lastName\": \"Taylor\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"creatorType\": \"editor\",\n" +
                "                \"firstName\": \"J\",\n" +
                "                \"lastName\": \"Rushton\"\n" +
                "            }\n" +
                "        ]";

        JsonNode creatorsNode = new ObjectMapper().readTree(creatorsChunk);

        List<String> creators = ZoteroUtil.parseCreators(creatorsNode);

        MatcherAssert.assertThat(creators.size(), Is.is(3));
        MatcherAssert.assertThat(creators.get(0), Is.is("FAO"));
    }

}