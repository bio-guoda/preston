package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class ZoteroFileStreamHandlerTest {

    @Test
    public void parseDate() {
        String s = ZoteroFileStreamHandler.parseDate("2023");
        assertThat(s, Is.is("2023"));
    }


    @Test
    public void parseDateYearMonth() {
        String s = ZoteroFileStreamHandler.parseDate("2023-12");
        assertThat(s, Is.is("2023-12"));
    }

    @Test
    public void parseDateYearMonthDay() {
        String s = ZoteroFileStreamHandler.parseDate("2023-12-24");
        assertThat(s, Is.is("2023-12-24"));
    }

    @Test
    public void parsePartlySupportedDateString() {
        String s = ZoteroFileStreamHandler.parseDate("MAY 2020");
        assertThat(s, Is.is("2020"));
    }

    @Test
    public void parsePartlySupportedDateString2() {
        String s = ZoteroFileStreamHandler.parseDate("2018-05-22T12:29:52Z");
        assertThat(s, Is.is("2018-05-22"));
    }

}