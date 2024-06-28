package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DarkTaxonFileStreamHandler implements ContentStreamHandler {

    public static final Map<String, String> TRANSLATION_MAP = new TreeMap<String, String>() {{
        put(".VN", "acceptedName");
        put(".FU", "originalSpecificEpithet");
        put(".OR", "originalGenus");
        put(".AU", "accordingTo");
        put(".FA", "family");
        put(".SF", "subfamily");
        put(".TR", "tribe");
        put(".ST", "subtribe");
        put(".IT", "infratribe");
        put(".GE", "genus");
        put(".SG", "subgenus");
        put(".GR", "speciesgroup");
        put(".SR", "speciessubgroup");
        put(".SC", "speciescomplex");
        put(".SS", "subspecies");
        put(".CO", "remarks");

    }};
    private static final String PREFIX_AUTHOR = ".A ";
    private static final String PREFIX_TITLE = ".S ";
    private static final String PREFIX_YEAR = ".J ";
    private static final String PREFIX_METHOD_DIGITIZATION = ".K ";
    private static final String PREFIX_JOURNAL = ".Z ";
    private static final String PREFIX_FILENAME = ".P ";
    public static final String LOCATIONS = "locations";
    public static final String DROS_5 = "taxodros-dros5";
    public static final String DROS_3 = "taxodros-dros3";
    public static final String SYS = "taxodros-syst";
    private static final String PREFIX_PUBLISHER = ".Z.";

    public static final String TAXODROS_DATA_DOI = "10.5281/zenodo.10723540";
    public static final String TAXODROS_DATA_VERSION_SHA256 = "hash://sha256/ca86d74b318a334bddbc7c6a387a09530a083b8617718f5369ad548744c602d3";
    public static final String TAXODROS_DATA_VERSION_MD5 = "hash://md5/26a67012dde325cf2a3a058cc2f9c1b8";
    public static final String LSID_PREFIX = "urn:lsid:taxodros.uzh.ch";
    public static final Pattern RAW_IMAGEFILE_PATTERN = Pattern.compile("(.*)/(?<plateId>[A-Z]+[0-9]+)_(?<specimenId>[A-Z]+[0-9]+)_(?<type>RAW)_(?<imageStackNumber>[0-9]+)_(?<imageNumber>[0-9]+)[.]tiff$");
    public static final Pattern STACKED_IMAGEFILE_PATTERN = Pattern.compile("(.*)/(?<plateId>[A-Z]+[0-9]+)_(?<specimenId>[A-Z]+[0-9]+)_(?<type>stacked)_(?<imageStackNumber>[0-9]+)[.]tiff$");

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    public static final Pattern HASH_AND_FILEPATH_PATTERN = Pattern.compile("(?<sha256hash>[a-f0-9]{64})\\s+(?<filepath>.*)");

    public DarkTaxonFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                      OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                int lineStart = -1;
                int lineFinish = -1;
                AtomicReference<StringBuilder> textCapture = new AtomicReference<>(new StringBuilder());

                BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));

                for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                    ObjectNode objectNode = new ObjectMapper().createObjectNode();
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }

                    // 72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822  BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff
                    Matcher matcher1 = HASH_AND_FILEPATH_PATTERN.matcher(line);

                    if (matcher1.matches()) {
                        String hash = matcher1.group("sha256hash");
                        String filepath = matcher1.group("filepath");

                        setOriginReference(iriString, lineNumber, lineNumber, objectNode);

                        Stream.of(RAW_IMAGEFILE_PATTERN, STACKED_IMAGEFILE_PATTERN)
                                .forEach(p -> {
                                    Matcher matcher = p.matcher(filepath);
                                    if (matcher.matches()) {
                                        populateFileObject(objectNode, hash, filepath, matcher);
                                        try {
                                            writeRecord(foundAtLeastOne, objectNode);
                                        } catch (IOException e) {
                                            //
                                        }
                                    }

                                });
                    }


                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    private void populateFileObject(ObjectNode objectNode, String hash, String filepath, Matcher matcher) {
        objectNode.put("darktaxon:plateId", matcher.group("plateId"));
        objectNode.put("darktaxon:specimenId", matcher.group("specimenId"));
        objectNode.put("darktaxon:imageStackNumber", matcher.group("imageStackNumber"));
        String type = matcher.group("type");
        objectNode.put("darktaxon:type", type);
        if (StringUtils.equalsIgnoreCase("raw", type)) {
            String imageNumber = matcher.group("imageNumber");
            if (StringUtils.isNotBlank(imageNumber)) {
                objectNode.put("darktaxon:imageNumber", matcher.group("imageNumber"));
            }
        }
        objectNode.put("darktaxon:hash", "hash://sha256/" + hash);
        objectNode.put("darktaxon:path", filepath);
        objectNode.put("darktaxon:mimeType", "image/tiff");
    }

    private List<String> collectCreators(String authorStrings) {
        String[] authorsFirstSplit = StringUtils.split(authorStrings, "&");
        List<String> creatorList = new ArrayList<>();

        for (String s : authorsFirstSplit) {
            String[] authors = StringUtils.split(s, ",");
            StringBuilder creatorName = new StringBuilder();
            for (int i = 0; i < authors.length; i++) {
                String author = StringUtils.trim(StringUtils.replace(authors[i], "&", ""));
                if (i % 2 == 0) {
                    creatorName.append(author);
                } else {
                    creatorName.append(", ");
                    creatorName.append(author);
                    creatorList.add(creatorName.toString());
                    creatorName = new StringBuilder();
                }
            }
        }
        return creatorList;
    }

    private void writeRecord(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        objectNode.put("description", "Uploaded by Plazi for Museum für Naturkunde Berlin.");
        ObjectNode metadata = new ObjectMapper().createObjectNode();
        metadata.set("metadata", objectNode);
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }


    public static void enrichWithJournalInfo(ObjectNode objectNode, String remainder) {
        Pattern articleCoordinates = Pattern.compile("(?<volume>[^\\(:]+){0,1}(?<issue>\\([^)]*\\)){0,1}:(?<pages>[^.]*){0,1}([ .]*)$");
        Matcher matcher = articleCoordinates.matcher(remainder);
        if (matcher.matches()) {
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_VOLUME, StringUtils.trim(matcher.group("volume")));
            ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_PAGES, matcher.group("pages"));
            String issue = matcher.group("issue");
            if (StringUtils.isNotBlank(issue)) {
                ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.JOURNAL_ISSUE, StringUtils.substring(issue, 1, issue.length() - 1));
            }
        }
    }

    private void setOriginReference(String iriString, int lineStart, int lineFinish, ObjectNode objectNode) {
        String suffix = lineFinish > lineStart
                ? "-" + "L" + lineFinish
                : "";
        String value = "line:" + iriString + "!/L" + lineStart + suffix;
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.WAS_DERIVED_FROM, value);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, "https://linker.bio/" + value);
        ZenodoMetaUtil.append(objectNode, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033 " +
                "\n" +
                "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");

    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
