package bio.guoda.preston.cmd;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

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

    @Test
    public void squareBracketsMark() {
        String s = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode("[question].txt");
        assertThat(s, Is.is("%5Bquestion%5D.txt"));
    }

    @Test
    public void squareClosingBracket() {
        String s = JavaScriptAndPythonFriendlyURLEncodingUtil
                .urlEncode("Thuiller et al. - 2006 - INTERACTIONS BETWEEN ENVIRONMENT, SPECIES TRAITS, .]");
        assertThat(s, Is.is("Thuiller%20et%20al.%20-%202006%20-%20INTERACTIONS%20BETWEEN%20ENVIRONMENT%2C%20SPECIES%20TRAITS%2C%20.%5D"));
    }

    @Test
    public void unreservedCharacters() {
        String unreservedCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~";
        assertThat(
                JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(unreservedCharacters),
                Is.is(unreservedCharacters)
        );
    }

    @Test
    public void handleSingleQuote() {
        String filename = "O'Leary et al. - 2013 - The Placental Mammal Ancestor and the Post–K-Pg Ra.pdf";
        String encoded = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(filename);
        assertThat(
                encoded,
                not(Is.is(filename))
        );
        assertThat(
                encoded,
                Is.is("O%26quot%3BLeary%20et%20al.%20-%202013%20-%20The%20Placental%20Mammal%20Ancestor%20and%20the%20Post%E2%80%93K-Pg%20Ra.pdf")
        );
    }

}