package bio.guoda.preston.cmd;


import bio.guoda.preston.process.ZoteroUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

public class ZoteroFileStreamHandlerTest {

    @Test
    public void parseDate() {
        String s = ZoteroUtil.parseDate("2023");
        assertThat(s, Is.is("2023"));
    }


    @Test
    public void parseDateYearMonth() {
        String s = ZoteroUtil.parseDate("2023-12");
        assertThat(s, Is.is("2023-12"));
    }

    @Test
    public void parseDateYearMonthDay() {
        String s = ZoteroUtil.parseDate("2023-12-24");
        assertThat(s, Is.is("2023-12-24"));
    }

    @Test
    public void parsePartlySupportedDateString() {
        String s = ZoteroUtil.parseDate("MAY 2020");
        assertThat(s, Is.is("2020"));
    }

    @Test
    public void parsePartlySupportedDateString2() {
        String s = ZoteroUtil.parseDate("2018-05-22T12:29:52Z");
        assertThat(s, Is.is("2018-05-22"));
    }

    @Test
    public void parseCreators() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("zotero/ZoteroArticleListMixedNameStructures.json"));
        assertNotNull(jsonNode);
        JsonNode creators = jsonNode.at("/data/creators");

        List<String> creatorsList = ZoteroUtil.parseCreators(creators);
        assertThat(creatorsList.size(), Is.is(5));
        assertThat(creatorsList.get(0), Is.is("Eric Moïse Bakwo Fils"));
        assertThat(creatorsList.get(2), Is.is("Ervis, Manfothang Dongmo"));

    }

}