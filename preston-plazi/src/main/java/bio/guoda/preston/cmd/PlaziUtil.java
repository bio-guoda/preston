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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlaziUtil {

    public static final String PATTERN_SUB_DISTRIBUTION = "([.].*Distribution[ .]+)";
    public static final Pattern PATTERN_BIBLIOGRAPHY = Pattern.compile("(.*)([.].*Bibliography[ .]+)(.*)");
    public static final Pattern PATTERN_TAXONOMY = Pattern.compile("(.*)(Taxonomy[ .]+)(.*)" + PATTERN_SUB_DISTRIBUTION + "(.*)");
    public static final Pattern PATTERN_DISTRIBUTION = Pattern.compile("(.*)" + PATTERN_SUB_DISTRIBUTION + "(.*)([.].*Descriptive notes[ .]+)(.*)");

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
            if (docAttributes.getNamedItem("masterDocId") != null) {
                String masterDocId = docAttributes.getNamedItem("masterDocId").getTextContent();
                treatment.put("docMasterId", "hash://md5/" + StringUtils.lowerCase(masterDocId));
            }

            if (docAttributes.getNamedItem("ID-ISBN") != null) {
                treatment.put("docISBN", docAttributes.getNamedItem("ID-ISBN").getTextContent());
            }
            if (docAttributes.getNamedItem("pageNumber") != null) {
                treatment.put("docPageNumber", docAttributes.getNamedItem("pageNumber").getTextContent());
            }

            String treatmentText = extractTreatment(docu);
            treatment.put("verbatimText", treatmentText);
            handleTaxonomy(treatment, treatmentText);
            if (treatment.has("commonNames")) {
                handleRemainingTreatment(treatment, docu, treatmentText);

            }
        }


        return treatment;
    }

    public static void handleRemainingTreatment(ObjectNode treatment, Document docu, String treatmentText) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        handleCommonNames(docu, treatment);
        handleNomenclature(docu, treatment);
        handleDistribution(treatmentText, treatment, docu);
        handleBibliography(treatment, treatmentText);
        handleFoodAndFeeding(treatment, docu);
        handleBreeding(treatment, docu);
        handleActivityPatterns(treatment, docu);
        handleMovements(treatment, docu);
        handleHabitat(treatment, docu);
        handleStatusAndConservation(treatment, docu);
        handleDescriptiveNotes(treatment, docu);
    }

    private static void handleDescriptiveNotes(ObjectNode treatment, Document docu) throws XPathExpressionException {
        NodeList emphasisNodes = XMLUtil.evaluateXPath(
                "//emphasis",
                docu
        );
        for (int i = 0; i < emphasisNodes.getLength(); i++) {
            Node emphasisNode = emphasisNodes.item(i);
            String textContent = emphasisNode.getTextContent();
            if (StringUtils.startsWith(textContent, "notes")) {
                Node expectedParagraphNode = emphasisNode.getParentNode();
                String descriptiveNotes = replaceTabsNewlinesWithSpaces(expectedParagraphNode);
                String[] descriptiveNotesMinusHabitat = StringUtils.splitByWholeSeparator(descriptiveNotes, "Habitat");
                String descriptiveNotesTrimmed = descriptiveNotesMinusHabitat[0];
                treatment.put("descriptiveNotes", StringUtils.trim(RegExUtils.replaceFirst(descriptiveNotesTrimmed, "Descriptive notes" + "[. ]+", "")));
            }
        }
    }

    private static void handleStatusAndConservation(ObjectNode treatment, Document docu) throws XPathExpressionException {
        NodeList emphasisNodes2 = XMLUtil.evaluateXPath(
                "//emphasis",
                docu
        );
        for (int i1 = 0; i1 < emphasisNodes2.getLength(); i1++) {
            Node emphasisNode1 = emphasisNodes2.item(i1);
            String textContent1 = replaceTabsNewlinesWithSpaces(emphasisNode1);
            if (StringUtils.startsWith(textContent1, "Status")) {
                Node expectedParagraphNode1 = emphasisNode1.getParentNode();
                String statusAndConservation = replaceTabsNewlinesWithSpaces(expectedParagraphNode1);
                treatment.put("statusAndConservation", StringUtils.trim(RegExUtils.replaceFirst(statusAndConservation, "Status and Conservation" + "[. ]+", "")));
            }
        }
    }

    private static void handleHabitat(ObjectNode treatment, Document docu) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        parseAttemptOfEmphasisSection(
                docu,
                treatment,
                "habitat",
                "Habitat", 1
        );
    }

    private static void handleMovements(ObjectNode treatment, Document docu) throws XPathExpressionException {
        NodeList emphasisNodes1 = XMLUtil.evaluateXPath(
                "//emphasis",
                docu
        );
        for (int i1 = 0; i1 < emphasisNodes1.getLength(); i1++) {
            Node emphasisNode1 = emphasisNodes1.item(i1);
            String textContent1 = replaceTabsNewlinesWithSpaces(emphasisNode1);
            String prefix = "Movements, Home range and Social organization.";
            if (StringUtils.startsWith(textContent1, prefix)) {
                Node expectedParagraphNode1 = emphasisNode1.getParentNode();
                String movementsHomeRangeAndSocialOrganization = replaceTabsNewlinesWithSpaces(expectedParagraphNode1);
                String[] movementsHomeRangeAndSocialOrganizationChunk = StringUtils.splitByWholeSeparator(movementsHomeRangeAndSocialOrganization, prefix);
                if (movementsHomeRangeAndSocialOrganizationChunk.length > 1) {
                    treatment.put("movementsHomeRangeAndSocialOrganization", StringUtils.trim(RegExUtils.replaceFirst(movementsHomeRangeAndSocialOrganizationChunk[1], "Movements," + "[. ]+", "")));
                }

            }
        }
    }

    private static void handleActivityPatterns(ObjectNode treatment, Document docu) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        parseAttemptOfEmphasisSection(
                docu,
                treatment,
                "activityPatterns",
                "Activity patterns", 1
        );
    }

    private static void handleBreeding(ObjectNode treatment, Document docu) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        parseAttemptOfEmphasisSection(
                docu,
                treatment,
                "breeding",
                "Breeding", 1
        );
    }

    private static void handleFoodAndFeeding(ObjectNode treatment, Document docu) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        parseAttemptOfEmphasisSection(
                docu,
                treatment,
                "foodAndFeeding",
                "Food and Feeding", 2
        );
    }

    static void handleBibliography(ObjectNode treatment, String treatmentText) throws XPathExpressionException {
        String bibliographyString = extractSegment(treatmentText, PATTERN_BIBLIOGRAPHY);

        String[] references = StringUtils.split(bibliographyString, ")");
        List<String> referenceList = new ArrayList<>();
        for (String reference : references) {
            String removeEndingPeriod = RegExUtils.replaceAll(reference, "^[ ,.]+", "");
            String individualReference = StringUtils.trim(RegExUtils.replaceAll(StringUtils.trim(removeEndingPeriod), "(eta/\\.|eta /\\.)", "et al."));
            if (StringUtils.isNoneBlank(individualReference)) {
                referenceList.add(individualReference + ")");
            }
        }
        treatment.put("bibliography", StringUtils.join(referenceList, " | "));
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

    private static void handleCommonNames(Document docu, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList commonNames = XMLUtil.evaluateXPath(
                "//subSubSection[@type='vernacular_names']/paragraph/emphasis",
                docu
        );

        List<String> commonNameList = new ArrayList<>();
        for (int i = 0; i < commonNames.getLength(); i++) {
            Node expectedCommonName = commonNames.item(i);
            String commonNameSegment = replaceTabsNewlinesWithSpaces(expectedCommonName.getParentNode());
            String[] commonNameSplit = StringUtils.split(commonNameSegment, ".");
            String commonNameTrimmed = commonNameSplit.length > 1 ? commonNameSplit[1] : commonNameSplit[0];
            String[] commonNamesSplit = StringUtils.split(commonNameTrimmed, "I/");
            for (String commonName : commonNamesSplit) {
                addCommonName(commonNameList, commonName);
            }
        }

        if (commonNameList.size() > 1) {
            treatment.put("commonNames", StringUtils.join(commonNameList, " | "));
        }

    }

    private static String addCommonName(List<String> commonNameList, String str) {
        String language = "en";
        String[] languageAndCommonName = StringUtils.split(str, ":");
        String commonNameWithoutLanguage = languageAndCommonName[0];
        if (languageAndCommonName.length > 1) {
            String languageString = languageAndCommonName[0];
            if (StringUtils.contains(languageString, "French")) {
                language = "fr";
            } else if (StringUtils.contains(languageString, "German")) {
                language = "de";
            } else if (StringUtils.contains(languageString, "Spanish")) {
                language = "es";
            } else if (StringUtils.contains(languageString, "Other common names")) {
                language = "en";
            }
            commonNameWithoutLanguage = languageAndCommonName[1];
        }

        String trim = StringUtils.trim(commonNameWithoutLanguage);
        Pattern compile = Pattern.compile("(.*)([a-z][A-Z])(.*)");
        Matcher matcher = compile.matcher(trim);
        if (matcher.matches()) {
            String group = matcher.group(2);
            trim = matcher.group(1) + group.substring(0, 1) + " " + group.substring(1) + matcher.group(3);
        }
        String commonNameWithISOLanguage = trim + " @" + language;
        if (!commonNameList.contains(commonNameWithISOLanguage)) {
            commonNameList.add(commonNameWithISOLanguage);
        }
        return language;
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

    private static void handleDistribution(String treatmentText, ObjectNode treatment, Document docu) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        String taxonomySegment = extractDistributionSegment(treatmentText);
        if (StringUtils.isNoneBlank(taxonomySegment)) {
            treatment.put("subspeciesAndDistribution", StringUtils.trim(taxonomySegment));
        }

        NodeList distribution = XMLUtil.evaluateXPath(
                "//subSubSection[@type='distribution']/caption/paragraph",
                docu
        );
        for (int i = 0; i < distribution.getLength(); i++) {
            Node distributionNode = distribution.item(i);
            Node parentNode = distributionNode.getParentNode();
            NamedNodeMap attributes = parentNode.getAttributes();
            if (attributes.getNamedItem("httpUri") != null) {
                treatment.put("distributionImageURL", attributes.getNamedItem("httpUri").getTextContent());
            }
        }
    }

    private static void handleTaxonomy(ObjectNode treatment, String treatmentText) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        String taxonomySegment = extractTaxonomySegment(treatmentText);
        if (StringUtils.isNoneBlank(taxonomySegment)) {
            treatment.put("taxonomy", StringUtils.trim(taxonomySegment));
        }
    }

    private static String extractTreatment(Document docu) throws XPathExpressionException {
        NodeList emphasisNodes = XMLUtil.evaluateXPath(
                "//treatment",
                docu
        );
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < emphasisNodes.getLength(); i++) {
            builder.append(emphasisNodes.item(i).getTextContent());
        }
        return StringUtils.trim(replaceTabsNewlinesWithSpaces(builder.toString()));
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

    public static String replaceTabsNewlinesWithSpaces(String textContent) {
        return StringUtils.trim(RegExUtils.replaceAll(textContent, "\\s+", " "));
    }

    public static String extractTaxonomySegment(String taxonomyText1) {
        return extractSegment(taxonomyText1, PATTERN_TAXONOMY);
    }

    public static String extractDistributionSegment(String taxonomyText1) {
        return extractSegment(taxonomyText1, PATTERN_DISTRIBUTION);
    }

    private static String extractSegment(String taxonomyText1, Pattern pattern) {
        Matcher matcher = pattern.matcher(taxonomyText1);
        String taxonomyText = null;
        if (matcher.matches()) {
            taxonomyText = replaceTabsNewlinesWithSpaces(matcher.group(3));
        }
        return taxonomyText + ".";
    }
}
