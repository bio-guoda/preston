package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderTaxonWorks extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("BIOCASE_XML_ARCHIVE", MimeTypes.MIME_TYPE_ABCDA);
        put("BIOCASE", MimeTypes.MIME_TYPE_BIOCASE_META);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};

    public static final String GBIF_API_OCCURRENCE_DOWNLOAD_PART = "//api.gbif.org/v1/occurrence/download";
    public static final String GBIF_OCCURRENCE_PART_PATH = "api.gbif.org/v1/occurrence";
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderTaxonWorks.class);
    public static final String TAXONWORKS_API_ENDPOINT = "https://sfg.taxonworks.org/api/v1";
    private static final String TAXONWORKS_CITATIONS = TAXONWORKS_API_ENDPOINT + "citations";
    public static final IRI TAXONWORKS_OPEN_PROJECTS = toIRI(TAXONWORKS_API_ENDPOINT);


    public RegistryReaderTaxonWorks(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.TAXONWORKS.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            List<Quad> nodes = new ArrayList<>();
            Stream.of(
                    toStatement(Seeds.TAXONWORKS, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderTaxonWorks.TAXONWORKS_OPEN_PROJECTS,
                            DESCRIPTION,
                            toEnglishLiteral("Provides an index of open projects hosted via TaxonWorks."))
            ).forEach(nodes::add);

            emitPageRequest(new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    nodes.add(statement);
                }
            }, TAXONWORKS_OPEN_PROJECTS);
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(TAXONWORKS_API_ENDPOINT)) {
            handleProjectIndex(statement);
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(TAXONWORKS_CITATIONS)) {
            handleCitations(statement);
        }
    }


    public void handleCitations(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseCitations(currentPage, new StatementsEmitterAdapter() {
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

    public void handleProjectIndex(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseProjectIndex(new StatementsEmitterAdapter() {
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

    static void emitNextPage(int pageNumber, int pageSize, StatementsEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "page=[0-9]*", "page=" + pageNumber);
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "per=[0-9]*", "per=" + pageSize);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "page") ? nextPageURL : nextPageURL + "&page=" + pageNumber;
        nextPageURL = StringUtils.contains(nextPageURL, "per") ? nextPageURL : nextPageURL + "&per=" + pageSize;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.TAXONWORKS),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }


    static void parseCitations(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            for (JsonNode node : jsonNode) {
                parseIndividualCitation(currentPage, emitter, node);
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    static void parseProjectIndex(StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {

        JsonNode jsonNode = new ObjectMapper().readTree(in);

        if (jsonNode.has("open_projects")) {
            for (JsonNode projectNode : jsonNode.get("open_projects")) {

                if (projectNode.isObject() && projectNode.size() > 0) {
                    Iterator<String> fieldNames = projectNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String projectToken = fieldNames.next();
                        IRI citations = toIRI(TAXONWORKS_API_ENDPOINT + "/citations/?citation_object_type=BiologicalAssociation&project_token=" + projectToken);
                        emitRequestForJSON(emitter, versionSource, citations);
                    }
                }
            }
        }

    }

    private static void emitRequestForJSON(StatementsEmitter emitter, IRI versionSource, IRI object) {
        emitter.emit(toStatement(versionSource, HAD_MEMBER, object));
        emitter.emit(toStatement(object, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
        emitter.emit(toStatement(object, HAS_VERSION, RefNodeFactory.toBlank()));
    }

    static void emitNextPageIfNeeded(StatementsEmitter emitter, IRI versionSource, JsonNode results) {
        if (!isEndOfRecords(results)
                && results.isArray()
                && results.size() > 0) {
            int pageNumber = extractPageNumber(versionSource);
            int pageSize = extractPageSize(versionSource, results);
            if (pageSize == results.size()) {
                String previousURL = versionSource.getIRIString();
                emitNextPage(pageNumber + 1, pageSize, emitter, previousURL);
            }
        }
    }

    private static int extractPageNumber(IRI versionSource) {
        Pattern compile = Pattern.compile("(.*)[?&]page=(?<pageNumber>[0-9]+)(.*)");
        Matcher matcher = compile.matcher(versionSource.getIRIString());
        return matcher.matches()
                ? Integer.parseInt(matcher.group("pageNumber"))
                : 0;
    }

    private static int extractPageSize(IRI versionSource, JsonNode jsonNode) {
        Pattern compile = Pattern.compile("(.*)[?&]per=(?<pageSize>[0-9]+)(.*)");
        Matcher matcher = compile.matcher(versionSource.getIRIString());
        return matcher.matches()
                ? Integer.parseInt(matcher.group("pageSize"))
                : jsonNode.size();
    }

    private static boolean isEndOfRecords(JsonNode jsonNode) {
        return jsonNode == null || jsonNode.size() == 0;
    }

    public static void parseIndividualCitation(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("source_id")) {
            String sourceId = result.get("source_id").asText();
            IRI sourceQuery = toIRI(TAXONWORKS_API_ENDPOINT + "/sources/" + sourceId);
            if (result.has("citation_object_id")) {
                emitter.emit(toStatement(currentPage, HAD_MEMBER, sourceQuery));
                emitter.emit(toStatement(sourceQuery, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                emitter.emit(toStatement(sourceQuery, HAS_VERSION, toBlank()));

                Pattern projectToken = Pattern.compile("(.*)([?&]project_token=)(?<projectToken>[a-zA-Z0-9]+)(.*)");
                String associationId = result.get("citation_object_id").asText();
                String associationsUrl = TAXONWORKS_API_ENDPOINT + "/biological_associations/" + associationId;
                Matcher matcher = projectToken.matcher(currentPage.getIRIString());
                if (matcher.matches()) {
                    associationsUrl = associationsUrl + "?project_token=" + matcher.group("projectToken");
                }

                IRI associationQuery = toIRI(associationsUrl);
                emitter.emit(toStatement(associationQuery, WAS_DERIVED_FROM, sourceQuery));
                emitter.emit(toStatement(associationQuery, HAS_VERSION, toBlank()));
            }
        }

    }


}
