package bio.guoda.preston.cmd;

import bio.guoda.preston.process.XMLUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.process.XMLUtil.compilePathExpression;
import static bio.guoda.preston.process.XMLUtil.evaluateXPath;

public class PlaziUtil {

    public static final String PATTERN_SUB_DISTRIBUTION = "([.].*Distribution[ .]+)";
    public static final String PATTERN_SUB_DESCRIPTIVE_NOTES = "([.].*Descriptive notes[ .]+)";
    public static final String PATTERN_SUB_HABITAT = "([.].*Habitat[ .]+)";
    public static final String PATTERN_SUB_MOVEMENTS = "([.].*Movements, Home range and Social organization[ .]+)";
    public static final String PATTERN_SUB_CONSERVATION = "([.].*Conservation.*[ .]+)";
    public static final Pattern PATTERN_BIBLIOGRAPHY = Pattern.compile("(.*)([.].*Bibliography[ .]+)(.*)");
    public static final Pattern PATTERN_TAXONOMY = Pattern.compile("(.*)(Taxonomy[ .]+)(.*)" + PATTERN_SUB_DISTRIBUTION + "(.*)");
    public static final Pattern PATTERN_DISTRIBUTION = Pattern.compile("(.*)" + PATTERN_SUB_DISTRIBUTION + "(.*)" + PATTERN_SUB_DESCRIPTIVE_NOTES + "(.*)");
    public static final Pattern PATTERN_DESCRIPTIVE_NOTES = Pattern.compile("(.*)" + PATTERN_SUB_DESCRIPTIVE_NOTES + "(.*)" + PATTERN_SUB_HABITAT + "(.*)");
    public static final Pattern PATTERN_MOVEMENTS = Pattern.compile("(.*)" + PATTERN_SUB_MOVEMENTS + "(.*)" + PATTERN_SUB_CONSERVATION + "(.*)");
    public static final Pattern DETECT_TAB_AND_NEWLINE = Pattern.compile("[\t\n]+");
    public static final Pattern DETECT_WHITESPACE = Pattern.compile("[\\s]+");
    public static final XPathExpression X_PATH_DOCUMENT = compilePathExpression("/document");
    public static final XPathExpression X_PATH_EMPHASIS = compilePathExpression("//emphasis");
    public static final XPathExpression X_PATH_TAXONOMIC_NAME = XMLUtil.compilePathExpression("//subSubSection[@type='nomenclature']//taxonomicName");
    public static final XPathExpression X_PATH_DISTRIBUTION = XMLUtil.compilePathExpression("//subSubSection[@type='distribution']/caption/paragraph");
    public static final XPathExpression X_PATH_TREATMENT = compilePathExpression("//treatment");
    public static final XPathExpression X_PATH_VERNACULAR = compilePathExpression("//subSubSection[@type='vernacular_names']//emphasis");

    public static ObjectNode parseTreatment(InputStream is) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        return parseTreatment(is, new ObjectMapper().createObjectNode());
    }

    public static ObjectNode parseTreatment(InputStream is, ObjectNode treatment) throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        Document docu = parseDocument(is);
        NodeList doc = evaluateXPath(docu, X_PATH_DOCUMENT);

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
            if (treatment.has("taxonomy")) {
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
        handleMovements(treatment, treatmentText);
        handleStatusAndConservation(treatment, docu);

        handleDescriptiveNotes(treatment, treatmentText);
        handleHabitat(treatment, docu);
    }

    static void handleMovements(ObjectNode treatment, String treatmentText) {
        String segment = extractMovementsSegment(treatmentText);
        if (StringUtils.isNoneBlank(segment)) {
            treatment.put("movementsHomeRangeAndSocialOrganization", segment);
        }
    }

    static void handleDescriptiveNotes(ObjectNode treatment, String treatmentText) {
        String descriptiveNoteSegment = extractDescriptiveNoteSegment(treatmentText);
        if (StringUtils.isNoneBlank(descriptiveNoteSegment)) {
            treatment.put("descriptiveNotes", descriptiveNoteSegment);
        }
    }

    private static void handleStatusAndConservation(ObjectNode treatment, Document docu) throws XPathExpressionException {
        NodeList emphasisNodes2 = evaluateXPath(
                docu, X_PATH_EMPHASIS
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
        NodeList emphasisNodes1 = evaluateXPath(
                docu, X_PATH_EMPHASIS
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
        return XMLUtil.parseDoc(is);
    }

    static void setIfNotNull(ObjectNode treatment, NamedNodeMap docAttributes, String attributeName) {
        if (docAttributes.getNamedItem(attributeName) != null) {
            treatment.put(attributeName, docAttributes.getNamedItem(attributeName).getTextContent());
        }
    }

    private static void handleCommonNames(Document docu, ObjectNode treatment) throws XPathExpressionException {
        NodeList commonNames = evaluateXPath(
                docu, X_PATH_VERNACULAR
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

    private static void addCommonName(List<String> commonNameList, String str) {
        String language = "en";
        String[] languageAndCommonName = StringUtils.split(str, ":");
        String commonNamesWithoutLanguage = languageAndCommonName[0];
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
            commonNamesWithoutLanguage = languageAndCommonName[1];
        }

        String[] commonNamesWithoutLanguageList = StringUtils.split(commonNamesWithoutLanguage, ",");
        for (String commonNameWithoutLanguage : commonNamesWithoutLanguageList) {
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
        }
    }


    private static void handleNomenclature(Document docu, ObjectNode treatment) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList taxonomicNames = evaluateXPath(
                docu, X_PATH_TAXONOMIC_NAME
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

        NodeList distribution = evaluateXPath(
                docu, X_PATH_DISTRIBUTION
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
        NodeList emphasisNodes = evaluateXPath(
                docu, X_PATH_TREATMENT
        );
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < emphasisNodes.getLength(); i++) {
            builder.append(emphasisNodes.item(i).getTextContent());
        }
        return StringUtils.trim(replaceTabsNewlinesWithSpaces(builder.toString()));
    }

    private static void parseAttemptOfEmphasisSection(Document docu, ObjectNode treatment, String label, String sectionText, int depth) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
        NodeList emphasisNodes = evaluateXPath(
                docu, X_PATH_EMPHASIS
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
        return StringUtils.trim(
                RegExUtils.replaceAll(
                        RegExUtils.replaceAll(textContent, DETECT_TAB_AND_NEWLINE, ""),
                        DETECT_WHITESPACE, " "));
    }

    public static String extractTaxonomySegment(String taxonomyText1) {
        return extractSegment(taxonomyText1, PATTERN_TAXONOMY);
    }

    public static String extractDescriptiveNoteSegment(String taxonomyText1) {
        return extractSegment(taxonomyText1, PATTERN_DESCRIPTIVE_NOTES);
    }

    public static String extractMovementsSegment(String text) {
        return extractSegment(text, PATTERN_MOVEMENTS);
    }

    public static String extractDistributionSegment(String text) {
        return extractSegment(text, PATTERN_DISTRIBUTION);
    }

    private static String extractSegment(String segmentCandidate, Pattern segmentPattern) {
        Matcher matcher = segmentPattern.matcher(segmentCandidate);
        return cleanText(matcher);
    }

    private static String cleanText(Matcher matcher) {
        String text = null;
        if (matcher.matches()) {
            text = matcher.group(3);
        }
        return StringUtils.isBlank(text) ? "" : text + ".";
    }

}
