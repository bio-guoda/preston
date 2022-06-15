package bio.guoda.preston.cmd;

import bio.guoda.preston.process.XMLUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class PlaziUtil {

    public static ObjectNode parseTreatment(InputStream is) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        return parseTreatment(is, new ObjectMapper().createObjectNode());
    }


    public static ObjectNode parseTreatment(InputStream is, ObjectNode objectNode) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        ObjectNode treatment = objectNode;
        Document docu = parseDocument(is);
        NodeList doc = XMLUtil.evaluateXPath(
                "/document",
                docu
        );

        if (doc.getLength() > 0) {

            Node document = doc.item(0);
            NamedNodeMap docAttributes = document.getAttributes();
            setIfNotNull(treatment, docAttributes, "docId");
            setIfNotNull(treatment, docAttributes, "docName");
            setIfNotNull(treatment, docAttributes, "docOrigin");
            if (docAttributes.getNamedItem("ID-ISBN") != null) {
                treatment.put("docISBN", docAttributes.getNamedItem("ID-ISBN").getTextContent());
            }
            if (docAttributes.getNamedItem("pageNumber") != null) {
                treatment.put("docPageNumber", docAttributes.getNamedItem("pageNumber").getTextContent());
            }

            handleNomenclature(docu, treatment);
            handleDistribution(docu, treatment);
            parseAttemptOfEmphasisSection(
                    docu,
                    treatment,
                    "eats",
                    "Food and Feeding", 2
            );
            parseAttemptOfEmphasisSection(
                    docu,
                    treatment,
                    "activity",
                    "Activity patterns", 1
            );
            parseAttemptOfEmphasisSection(
                    docu,
                    treatment,
                    "bibliography",
                    "Bibliography", 2
            );

            parseAttemptOfEmphasisSection(
                    docu,
                    treatment,
                    "habitat",
                    "Habitat", 1
            );
        }


        return treatment;
    }

    static Document parseDocument(InputStream is) throws IOException, ParserConfigurationException, SAXException {
        String s = IOUtils.toString(is, StandardCharsets.UTF_8);

        return XMLUtil.parseDoc(IOUtils.toInputStream(s, StandardCharsets.UTF_8));
    }

    static void setIfNotNull(ObjectNode treatment, NamedNodeMap docAttributes, String attributeName) {
        if (docAttributes.getNamedItem(attributeName) != null) {
            treatment.put(attributeName, docAttributes.getNamedItem(attributeName).getTextContent());
        }
    }

    private static void handleNomenclature(Document docu, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList taxonomicNames = XMLUtil.evaluateXPath(
                "//subSubSection[@type='nomenclature']/paragraph/taxonomicName",
                docu
        );

        if (taxonomicNames.getLength() > 0) {
            Node expectedTaxonomicName = taxonomicNames.item(0);
            NamedNodeMap attributes = expectedTaxonomicName.getAttributes();
            for (int j = 0; j < attributes.getLength(); j++) {
                Node attribute = attributes.item(j);
                String nodeName = attribute.getNodeName();
                if (!StringUtils.equals(nodeName, "box")) {
                    treatment.put("interpreted" + StringUtils.capitalize(nodeName), attribute.getTextContent());
                }
            }
            String nameWithoutNewlines = replaceTabsNewlinesWithSpaces(expectedTaxonomicName);
            treatment.put("name", nameWithoutNewlines);
        }
    }

    private static void handleDistribution(Document docu, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList distribution = XMLUtil.evaluateXPath(
                "//subSubSection[@type='distribution']/caption/paragraph",
                docu
        );
        for (int i = 0; i < distribution.getLength(); i++) {
            Node distributionNode = distribution.item(i);
            String textContent = distributionNode.getTextContent();
            treatment.put("distribution", replaceTabsNewlinesWithSpaces(StringUtils.replace(textContent, "Distribution.", "")));

            Node parentNode = distributionNode.getParentNode();
            NamedNodeMap attributes = parentNode.getAttributes();
            if (attributes.getNamedItem("httpUri") != null) {
                treatment.put("distributionImageURL", attributes.getNamedItem("httpUri").getTextContent());
            }
        }
    }

    private static void parseAttemptOfEmphasisSection(Document docu, ObjectNode treatment, String label, String sectionText, int depth) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList emphasisNodes = XMLUtil.evaluateXPath(
                "//emphasis",
                docu
        );
        for (int i = 0; i < emphasisNodes.getLength(); i++) {
            Node emphasisNode = emphasisNodes.item(i);
            String textContent = emphasisNode.getTextContent();
            if (StringUtils.startsWith(textContent, sectionText)) {
                Node expectedParagraphNode = depth == 2
                        ? emphasisNode.getParentNode().getParentNode()
                        : emphasisNode.getParentNode();
                String eatingText = replaceTabsNewlinesWithSpaces(expectedParagraphNode);
                treatment.put(label, StringUtils.trim(RegExUtils.replaceFirst(eatingText, sectionText + "[. ]+", "")));
            }
        }
    }

    private static String replaceTabsNewlinesWithSpaces(Node expectedParagraphNode) {
        String textContent = expectedParagraphNode.getTextContent();
        return replaceTabsNewlinesWithSpaces(textContent);
    }

    private static String replaceTabsNewlinesWithSpaces(String textContent) {
        return StringUtils.trim(RegExUtils.replaceAll(textContent, "\\s+", " "));
    }

}
