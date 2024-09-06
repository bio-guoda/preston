package bio.guoda.preston.cmd;

import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import bio.guoda.preston.zenodo.ZenodoConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.list.TreeList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DarkTaxonFileStreamHandler implements ContentStreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DarkTaxonFileStreamHandler.class);

    public static final Pattern RAW_IMAGEFILE_PATTERN = Pattern.compile("(.*)/(?<plateId>[A-Z]+[0-9]+)_(?<specimenId>[A-Z]+[0-9]+)_(?<imageAcquisitionMethod>RAW)_(?<imageStackNumber>[0-9]+)_(?<imageNumber>[0-9]+)[.]tiff$");
    public static final Pattern STACKED_IMAGEFILE_PATTERN = Pattern.compile("(.*)/(?<plateId>[A-Z]+[0-9]+)_(?<specimenId>[A-Z]+[0-9]+)_(?<imageAcquisitionMethod>stacked)_(?<imageStackNumber>[0-9]+)[.]tiff$");
    public static final String IMAGE_CONTENT_ID = "darktaxon:imageContentId";
    private final List<String> communities;
    private final ZenodoConfig ctx;
    private PublicationDateFactory publicationDateFactory;

    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    public static final Pattern HASH_AND_FILEPATH_PATTERN = Pattern.compile("(?<sha256hash>[a-f0-9]{64})\\s+(?<imageFilepath>.*)/(?<imageFilename>[^/]+$)");

    public DarkTaxonFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                      OutputStream os,
                                      ZenodoConfig ctx,
                                      PublicationDateFactory publicationDateFactory) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.ctx = ctx;
        this.communities = ctx.getCommunities();
        this.publicationDateFactory = publicationDateFactory;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                TreeMap<String, List<String>> rawImagesByStack = new TreeMap<>();
                TreeMap<String, String> idForStack = new TreeMap<>();

                BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));

                for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                    ObjectNode objectNode = new ObjectMapper().createObjectNode();
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }

                    try {
                        JsonNode jsonNode = new ObjectMapper().readTree(line);
                        String fieldName = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
                        if (jsonNode.has(fieldName)) {
                            String type = jsonNode.get(fieldName).asText();
                            if ("http://rs.tdwg.org/ac/terms/Multimedia".equals(type)) {
                                writeDeposit(DarkTaxonUtil.toPhotoDeposit(jsonNode, publicationDateFactory, communities));
                            } else if ("http://rs.tdwg.org/dwc/terms/Occurrence".equals(type)) {
                                writeDeposit(DarkTaxonUtil.toPhysicalObjectDeposit(withNewline(line), publicationDateFactory, communities));
                            } else if ("http://rs.tdwg.org/dwc/terms/Event".equals(type)) {
                                writeDeposit(DarkTaxonUtil.toEventDeposit(withNewline(line), publicationDateFactory, communities));
                            }
                         }
                    } catch (IOException e) {
                        // opportunistic parsing
                    } catch (MissingMetadataFieldException e) {
                        LOG.warn("not processing following line because of missing data: [" + line + "]", e);
                    }


                    // example of matching line:
                    //
                    // 72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822  BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff
                    //
                    Matcher matcher1 = HASH_AND_FILEPATH_PATTERN.matcher(line);

                    if (matcher1.matches()) {
                        String hash = matcher1.group("sha256hash");
                        String imageFilename = matcher1.group("imageFilename");
                        String imageFilepath = matcher1.group("imageFilepath") + "/" + imageFilename;

                        setOriginReference(iriString, lineNumber, lineNumber, objectNode);

                        Stream.of(RAW_IMAGEFILE_PATTERN, STACKED_IMAGEFILE_PATTERN)
                                .forEach(p -> {
                                    Matcher matcher = p.matcher(imageFilepath);
                                    if (matcher.matches()) {
                                        populateFileObject(objectNode, hash, imageFilepath, matcher, rawImagesByStack, idForStack, imageFilename);
                                        try {
                                            writeZenodoMetadata(foundAtLeastOne, objectNode);
                                        } catch (IOException e) {
                                            //
                                        }
                                    }

                                });
                    }


                }
                for (Map.Entry<String, List<String>> entry : rawImagesByStack.entrySet()) {
                    ObjectNode linkRecords = new ObjectMapper().createObjectNode();
                    setOriginReference(iriString, linkRecords);
                    String imageContentId = idForStack.get(entry.getKey());
                    linkRecords.put(IMAGE_CONTENT_ID, imageContentId);
                    DarkTaxonUtil.appendAlternateIdentifiers(linkRecords, imageContentId);
                    ArrayNode arrayNode = new ObjectMapper().createArrayNode();
                    entry.getValue().forEach(arrayNode::add);
                    linkRecords.set(ZenodoMetaUtil.IS_DERIVED_FROM, arrayNode);

                    List<String> rawImages = entry.getValue();
                    for (String rawImage : rawImages) {
                        ZenodoMetaUtil.appendIdentifier(linkRecords, ZenodoMetaUtil.IS_DERIVED_FROM, rawImage);
                    }
                    DarkTaxonUtil.setDescription(linkRecords, "Uploaded by Plazi for the Museum für Naturkunde Berlin.");
                    writeZenodoMetadata(foundAtLeastOne, linkRecords);
                }
            }
        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }


        return foundAtLeastOne.get();
    }

    private String withNewline(String line) {
        return line + "\n";
    }

    private void populateFileObject(ObjectNode objectNode,
                                    String hash,
                                    String imageFilepath,
                                    Matcher matcher,
                                    Map<String, List<String>> rawImagesByStack,
                                    Map<String, String> idForStack,
                                    String imageFilename) {
        String plateId = matcher.group("plateId");
        String specimenId = matcher.group("specimenId");
        String imageStackNumber = matcher.group("imageStackNumber");

        objectNode.put("darktaxon:plateId", plateId);
        objectNode.put("darktaxon:specimenId", specimenId);

        objectNode.put("darktaxon:imageStackNumber", imageStackNumber);
        String acquisitionMethod = matcher.group("imageAcquisitionMethod");
        objectNode.put("darktaxon:imageAcquisitionMethod", acquisitionMethod);

        String imageContentId = "hash://sha256/" + hash;

        objectNode.put(IMAGE_CONTENT_ID, imageContentId);

        String imageStackId = plateId + "_" + specimenId + "_stacked_" + imageStackNumber;
        ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_AC_CAPTURE_DEVICE, "digital camera");
        if (StringUtils.equals("stacked", matcher.group("imageAcquisitionMethod"))) {
            idForStack.put(imageStackId, imageContentId);
            ZenodoMetaUtil.addCustomField(objectNode, ZenodoMetaUtil.FIELD_CUSTOM_AC_RESOURCE_CREATION_TECHNIQUE, "focus stacking");
        }


        String mimeType = "image/tiff";
        List<String> creators = Arrays.asList("Museum für Naturkunde Berlin");
        DarkTaxonUtil.populatePhotoDepositMetadata(objectNode, imageFilename, specimenId, imageContentId, mimeType, publicationDateFactory, communities, "Photo of Specimen " + specimenId, "Uploaded by Plazi for the Museum für Naturkunde Berlin.", creators);

        if (StringUtils.equals("RAW", acquisitionMethod)) {
            String imageNumber = matcher.group("imageNumber");
            if (StringUtils.isNotBlank(imageNumber)) {
                objectNode.put("darktaxon:imageNumber", matcher.group("imageNumber"));
            }
            rawImagesByStack.putIfAbsent(imageStackId, new TreeList<>());
            List<String> rawImages = rawImagesByStack.getOrDefault(imageStackId, new ArrayList<>());
            rawImages.add(imageContentId);
            rawImagesByStack.put(imageStackId, rawImages);
        }
        objectNode.put("darktaxon:imageFilepath", imageFilepath);
        objectNode.put("darktaxon:mimeType", mimeType);

    }

    private void writeZenodoMetadata(AtomicBoolean foundAtLeastOne, ObjectNode objectNode) throws IOException {
        ObjectNode metadata = ZenodoMetaUtil.wrap(objectNode);
        writeDeposit(metadata);
        objectNode.removeAll();
        foundAtLeastOne.set(true);
    }

    private void writeDeposit(ObjectNode metadata) throws IOException {
        IOUtils.copy(IOUtils.toInputStream(metadata.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
    }


    private void setOriginReference(String iriString, ObjectNode objectNode) {
        setOriginReference(iriString, -1, -1, objectNode);
    }

    private void setOriginReference(String iriString, int lineStart, int lineFinish, ObjectNode objectNode) {
        String suffix = lineFinish > lineStart
                ? "-" + "L" + lineFinish
                : "";
        String value = (StringUtils.isBlank(suffix) && lineStart == -1) ? iriString : ("line:" + iriString + "!/L" + lineStart + suffix);
        ZenodoMetaUtil.setValue(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, value);
        ZenodoMetaUtil.appendIdentifier(objectNode, ZenodoMetaUtil.IS_DERIVED_FROM, "https://linker.bio/" + value);
        ZenodoMetaUtil.append(objectNode, ZenodoMetaUtil.REFERENCES, "Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033");
        ZenodoMetaUtil.append(objectNode, ZenodoMetaUtil.REFERENCES, "Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14");

    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


    public void setPublicationDateFactory(PublicationDateFactory publicationDateFactory) {
        this.publicationDateFactory = publicationDateFactory;
    }
}
