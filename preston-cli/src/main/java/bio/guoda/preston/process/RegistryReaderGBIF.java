package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DEPICTS;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderGBIF extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("BIOCASE_XML_ARCHIVE", MimeTypes.MIME_TYPE_ABCDA);
        put("BIOCASE", MimeTypes.MIME_TYPE_BIOCASE_META);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};

    public static final String GBIF_API_DATASET_PART = "//api.gbif.org/v1/dataset";
    public static final String GBIF_API_OCCURRENCE_DOWNLOAD_PART = "//api.gbif.org/v1/occurrence/download";
    public static final String GBIF_OCCURRENCE_PART_PATH = "api.gbif.org/v1/occurrence";
    public static final String GBIF_OCCURRENCE_PART = "//" + GBIF_OCCURRENCE_PART_PATH;
    public static final String GBIF_OCCURRENCE_SEARCH = "https:" + GBIF_OCCURRENCE_PART + "/search";
    public static final String GBIF_DATASET_REGISTRY_STRING = "https:" + GBIF_API_DATASET_PART;
    public static final String GBIF_OCCURRENCE_STRING = "https:" + GBIF_OCCURRENCE_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderGBIF.class);
    public static final IRI GBIF_REGISTRY = toIRI(GBIF_DATASET_REGISTRY_STRING);

    public static final Pattern OCCURRENCE_RECORD_URL_PATTERN = Pattern.compile("<http[s]{0,1}://" + GBIF_OCCURRENCE_PART_PATH + "/(([0-9]+)|(search.*))>");

    public RegistryReaderGBIF(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    public static boolean isOccurrenceRecordEndpoint(BlankNodeOrIRI subject) {
        return OCCURRENCE_RECORD_URL_PATTERN.matcher(subject.ntriplesString()).matches();
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.GBIF.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            List<Quad> nodes = new ArrayList<>();
            Stream.of(
                    toStatement(Seeds.GBIF, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderGBIF.GBIF_REGISTRY,
                            DESCRIPTION,
                            toEnglishLiteral("Provides a registry of Darwin Core archives, and EML descriptors."))
            ).forEach(nodes::add);

            emitPageRequest(new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, GBIF_REGISTRY);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(GBIF_API_DATASET_PART)) {
            handleDataset(statement);
        } else if (hasVersionAvailable(statement)
                && isOccurrenceDownload(statement)) {
            handleOccurrenceDownload(statement);
        } else if (hasVersionAvailable(statement)
                && isOccurrenceRecordEndpoint(statement)) {
            handleOccurrenceRecords(statement);
        }
    }

    private boolean isOccurrenceRecordEndpoint(Quad statement) {
        return isOccurrenceRecordEndpoint(getVersionSource(statement));
    }

    private boolean isOccurrenceDownload(Quad statement) {
        return getVersionSource(statement).toString().contains(GBIF_API_OCCURRENCE_DOWNLOAD_PART)
                && !getVersionSource(statement).toString().contains("/download/request/");
    }

    public void handleOccurrenceDownload(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseOccurrenceDownload(currentPage, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                }, is, getVersionSource(statement));
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
        ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
    }

    private void handleOccurrenceRecords(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseOccurrenceRecords(currentPage, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                }, is, getVersionSource(statement));
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
        ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
    }

    public void handleDataset(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseDatasetResultPage(currentPage, new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                }, is, getVersionSource(statement));
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
        ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
    }

    static void emitNextPage(int offset, int limit, StatementsEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        nextPageURL = StringUtils.replacePattern(nextPageURL, "limit=[0-9]*", "limit=" + limit);
        nextPageURL = StringUtils.replacePattern(nextPageURL, "offset=[0-9]*", "offset=" + offset);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "offset") ? nextPageURL : nextPageURL + "&offset=" + offset;
        nextPageURL = StringUtils.contains(nextPageURL, "limit") ? nextPageURL : nextPageURL + "&limit=" + limit;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.GBIF),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parseOccurrenceRecords(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("results")) {
                for (JsonNode result : jsonNode.get("results")) {
                    requestIndividualOccurrence(currentPage, emitter, result);
                }
            } else if (jsonNode.has("key")) {
                parseOccurrenceRecord(emitter, jsonNode);
            } else if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    requestIndividualOccurrence(currentPage, emitter, node);
                }
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    static void parseDatasetResultPage(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("results")) {
                for (JsonNode result : jsonNode.get("results")) {
                    parseIndividualDataset(currentPage, emitter, result);
                }
            } else if (jsonNode.has("key")) {
                parseIndividualDataset(currentPage, emitter, jsonNode);
            } else if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    parseIndividualDataset(currentPage, emitter, node);
                }
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    private static void emitNextPageIfNeeded(StatementsEmitter emitter, IRI versionSource, JsonNode jsonNode) {
        if (!isEndOfRecords(jsonNode)
                && jsonNode.has("count")
                && jsonNode.has("offset")
                && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            if (offset == 0) {
                int totalNumberOfRecords = jsonNode.get("count").asInt();
                int limit = jsonNode.get("limit").asInt();
                String previousURL = versionSource.getIRIString();
                for (int i = limit; i < totalNumberOfRecords; i += limit) {
                    emitNextPage(i, limit, emitter, previousURL);
                }
            }
        }
    }

    private static boolean isEndOfRecords(JsonNode jsonNode) {
        return jsonNode == null
                || (!jsonNode.has("endOfRecords") || jsonNode.get("endOfRecords").asBoolean(true));
    }

    static void parseOccurrenceDownload(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.has("downloadLink")) {
                String downloadLink = jsonNode.get("downloadLink").asText();
                IRI downloadLinkIRI = toIRI(downloadLink);
                emitter.emit(toStatement(currentPage, HAD_MEMBER, downloadLinkIRI));
                emitter.emit(toStatement(downloadLinkIRI, HAS_VERSION, toBlank()));

                if (jsonNode.has("doi")) {
                    String doi = jsonNode.get("doi").asText();
                    IRI doiURL = toIRI("https://doi.org/" + doi);
                    emitter.emit(toStatement(currentPage, HAD_MEMBER, doiURL));
                    emitter.emit(toStatement(downloadLinkIRI, SEE_ALSO, doiURL));
                }

            }

        }
    }


    public static void requestIndividualOccurrence(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("key")) {
            String key = result.get("key").asText();
            IRI occurrenceKey = toIRI(GBIF_OCCURRENCE_STRING + "/" + key);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, occurrenceKey));
            emitter.emit(toStatement(toIRI(GBIF_OCCURRENCE_STRING + "/" + key), HAS_VERSION, toBlank()));
        }
    }

    public static void parseOccurrenceRecord(StatementsEmitter emitter, JsonNode result) {
        if (result.has("key")) {
            String key = result.get("key").asText();
            IRI occurrenceKey = toIRI(GBIF_OCCURRENCE_STRING + "/" + key);

            if (result.has("media")) {
                handleMedia(emitter, result, occurrenceKey);
            }
        }
    }

    public static void parseIndividualDataset(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("key")) {
            String uuid = result.get("key").asText();
            IRI datasetUUID = toIRI(uuid);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetUUID));
            if (result.has("doi")) {
                String doi = result.get("doi").asText();
                emitter.emit(toStatement(datasetUUID, SEE_ALSO, toIRI("https://doi.org/" + doi)));
            }

            if (result.has("endpoints")) {
                handleEndpoints(emitter, result, datasetUUID);
            } else {
                emitter.emit(toStatement(toIRI(GBIF_DATASET_REGISTRY_STRING + "/" + uuid), HAS_VERSION, toBlank()));
            }
        }
    }

    public static void handleMedia(StatementsEmitter emitter, JsonNode result, IRI occurrenceUUID) {
        for (JsonNode media : result.get("media")) {
            if (media.has("identifier")
                    && media.has("type")
                    && media.has("format")) {
                String urlString = media.get("identifier").asText();
                String type = media.get("type").asText();

                if ("StillImage".equals(type)) {
                    IRI imageUrl = toIRI(urlString);
                    emitter.emit(toStatement(occurrenceUUID, HAD_MEMBER, imageUrl));
                    emitter.emit(toStatement(imageUrl, DEPICTS, occurrenceUUID));
                    emitter.emit(toStatement(imageUrl, HAS_FORMAT, toContentType(media.get("format").asText())));
                    emitter.emit(toStatement(imageUrl, HAS_VERSION, toBlank()));
                }
            }
        }
    }

    public static void handleEndpoints(StatementsEmitter emitter, JsonNode result, IRI datasetUUID) {
        for (JsonNode endpoint : result.get("endpoints")) {
            if (endpoint.has("url") && endpoint.has("type")) {
                String urlString = endpoint.get("url").asText();
                String type = endpoint.get("type").asText();

                if (SUPPORTED_ENDPOINT_TYPES.containsKey(type)) {
                    IRI dataArchive = toIRI(urlString);
                    emitter.emit(toStatement(datasetUUID, HAD_MEMBER, dataArchive));
                    emitter.emit(toStatement(dataArchive, HAS_FORMAT, toContentType(SUPPORTED_ENDPOINT_TYPES.get(type))));
                    emitter.emit(toStatement(dataArchive, HAS_VERSION, toBlank()));
                }
            }
        }
    }

}
