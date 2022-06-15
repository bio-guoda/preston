package bio.guoda.preston.cmd;

import bio.guoda.preston.process.XMLUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
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
        String s = IOUtils.toString(is, StandardCharsets.UTF_8);

        NodeList doc = XMLUtil.evaluateXPath(
                "/document",
                IOUtils.toInputStream(s, StandardCharsets.UTF_8)
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

            handleNomenclature(s, treatment);
            handleDistribution(s, treatment);
            parseAttemptOfEmphasisSection(
                    s,
                    treatment,
                    "eats",
                    "Food and Feeding", 2
            );
            parseAttemptOfEmphasisSection(
                    s,
                    treatment,
                    "activity",
                    "Activity patterns", 1
            );
            parseAttemptOfEmphasisSection(
                    s,
                    treatment,
                    "bibliography",
                    "Bibliography", 2
            );

            parseAttemptOfEmphasisSection(
                    s,
                    treatment,
                    "habitat",
                    "Habitat", 1
            );
        }


        return treatment;
    }

    static void setIfNotNull(ObjectNode treatment, NamedNodeMap docAttributes, String attributeName) {
        if (docAttributes.getNamedItem(attributeName) != null) {
            treatment.put(attributeName, docAttributes.getNamedItem(attributeName).getTextContent());
        }
    }

    private static void handleNomenclature(String s, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList taxonomicNames = XMLUtil.evaluateXPath(
                "//subSubSection[@type='nomenclature']/paragraph/taxonomicName",
                IOUtils.toInputStream(s, StandardCharsets.UTF_8)
        );
        for (int i = 0; i < taxonomicNames.getLength(); i++) {
            Node expectedTaxonomicName = taxonomicNames.item(i);
            NamedNodeMap attributes = expectedTaxonomicName.getAttributes();
            for (int j = 0; j < attributes.getLength(); j++) {
                Node attribute = attributes.item(j);
                String nodeName = attribute.getNodeName();
                if (!StringUtils.equals(nodeName, "box")) {
                    treatment.put(nodeName, attribute.getTextContent());
                }
            }
        }
    }

    private static void handleDistribution(String s, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList distribution = XMLUtil.evaluateXPath(
                "//subSubSection[@type='distribution']/caption/paragraph",
                IOUtils.toInputStream(s, StandardCharsets.UTF_8)
        );
        for (int i = 0; i < distribution.getLength(); i++) {
            Node distributionNode = distribution.item(i);
            String textContent = distributionNode.getTextContent();
            treatment.put("distribution", StringUtils.trim(StringUtils.replace(textContent, "Distribution.", "")));

            Node parentNode = distributionNode.getParentNode();
            NamedNodeMap attributes = parentNode.getAttributes();
            if (attributes.getNamedItem("httpUri") != null) {
                treatment.put("distributionImageURL", attributes.getNamedItem("httpUri").getTextContent());
            }
        }
    }

    private static void parseAttemptOfEmphasisSection(String s, ObjectNode treatment, String label, String sectionText, int depth) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList emphasisNodes = XMLUtil.evaluateXPath(
                "//emphasis",
                IOUtils.toInputStream(s, StandardCharsets.UTF_8)
        );
        for (int i = 0; i < emphasisNodes.getLength(); i++) {
            Node emphasisNode = emphasisNodes.item(i);
            String textContent = emphasisNode.getTextContent();
            if (StringUtils.startsWith(textContent, sectionText)) {
                Node expectedParagraphNode = depth == 2
                        ? emphasisNode.getParentNode().getParentNode()
                        : emphasisNode.getParentNode();
                String eatingText = RegExUtils.replaceAll(expectedParagraphNode.getTextContent(), "\\s+", " ");
                treatment.put(label, StringUtils.trim(RegExUtils.replaceFirst(eatingText, sectionText + "[. ]+", "")));
            }
        }
    }

}
