package bio.guoda.preston.process;

import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;

public interface XPathHandler {
    void evaluateXPath(StatementsEmitter emitter, NodeList evaluate) throws XPathExpressionException;
}
