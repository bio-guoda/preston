package org.globalbioticinteractions.preston.process;

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
            public void evaluateXPath(StatementEmitter emitter, NodeList evaluate) throws XPathExpressionException {

            }
        }, statement -> {

        }, getNonXMLErrorPage());
    }

    private InputStream getNonXMLErrorPage() {
        return getClass().getResourceAsStream("error.html");
    }


}