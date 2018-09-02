package org.globalbioticinteractions.preston.process;

import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathExpressionException;

public interface XPathHandler {
    void evaluateXPath(StatementEmitter emitter, NodeList evaluate) throws XPathExpressionException;
}
