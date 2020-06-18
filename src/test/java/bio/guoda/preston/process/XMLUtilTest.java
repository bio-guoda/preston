package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class XMLUtilTest {

    @Ignore
    @Test(timeout = 5000)
    public void feedNonXMLErrorPage() throws IOException {
        XMLUtil.handleXPath("//", new XPathHandler() {
            @Override
            public void evaluateXPath(StatementsEmitter emitter, NodeList evaluate) throws XPathExpressionException {

            }
        }, new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {

            }
        }, getNonXMLErrorPage());
    }

    private InputStream getNonXMLErrorPage() {
        return getClass().getResourceAsStream("error.html");
    }


}