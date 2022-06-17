package bio.guoda.preston.process;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class XMLUtil {

    public static void handleXPath(String expression,
                                   XPathHandler handler,
                                   StatementsEmitter emitter,
                                   InputStream resourceAsStream) throws IOException {
        try {
            NodeList resultList = evaluateXPath(expression, resourceAsStream);
            handler.evaluateXPath(emitter, resultList);
        } catch (XPathExpressionException | ParserConfigurationException | IOException | SAXException e) {
            throw new IOException("failed to handle xpath", e);
        }
    }

    public static NodeList evaluateXPath(String expression, InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        Document doc = parseDoc(resourceAsStream);
        return evaluateXPath(doc, expression);
    }

    public static Document parseDoc(InputStream resourceAsStream) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(resourceAsStream);
    }

    public static NodeList evaluateXPath(Document doc, String expression) throws XPathExpressionException {
        XPathExpression expr = compilePathExpression(expression);
        return evaluateXPath(doc, expr);
    }

    public static NodeList evaluateXPath(Document doc, XPathExpression expr) throws XPathExpressionException {
        return (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
    }

    public static XPathExpression compilePathExpression(String expression)  {
        XPathFactory xPathfactory = XPathFactory.newInstance();
        XPath xpath = xPathfactory.newXPath();
        xpath.setNamespaceContext(getNsContext());
        try {
            return xpath.compile(expression);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("suspicious xpath expression [" + expression + "]", e);
        }
    }

    private static NamespaceContext getNsContext() {
        return new NamespaceContext() {
            @Override
            public String getNamespaceURI(String prefix) {
                Map<String, String> namespaceMap = new HashMap<String, String>() {{
                    put("dsi","http://www.biocase.org/schemas/dsi/1.0");
                    put("xml","http://www.biocase.org/schemas/dsi/1.0");
                }};
                return namespaceMap.getOrDefault(prefix, XMLConstants.NULL_NS_URI);
            }

            @Override
            public String getPrefix(String namespaceURI) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator getPrefixes(String namespaceURI) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
