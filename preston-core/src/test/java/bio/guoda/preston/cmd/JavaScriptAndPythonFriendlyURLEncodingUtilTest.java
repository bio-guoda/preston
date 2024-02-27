package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class JavaScriptAndPythonFriendlyURLEncodingUtilTest {

    @Test
    public void ampersand() {
        String s = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode("space & name & comma.txt");
        assertThat(s, Is.is("space%20%26%20name%20%26%20comma.txt"));
    }

    @Test
    public void comma() {
        String s = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode("space, comma.txt");
        assertThat(s, Is.is("space%2C%20comma.txt"));
    }

    @Test
    public void questionMark() {
        String s = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode("space ? question.txt");
        assertThat(s, Is.is("space%20%3F%20question.txt"));
    }

}