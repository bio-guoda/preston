package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderTaxonWorks.class);
    public static final String TAXONWORKS_API_ENDPOINT = "https://sfg.taxonworks.org/api/v1";
    private static final String TAXONWORKS_CITATIONS = TAXONWORKS_API_ENDPOINT + "/citations";
    private static final String TAXONWORKS_BIOLOGICAL_ASSOCIATIONS = TAXONWORKS_API_ENDPOINT + "/biological_associations";
    private static final String TAXONWORKS_OTUS = TAXONWORKS_API_ENDPOINT + "/otus";
    private static final String TAXONWORKS_TAXON_NAMES = TAXONWORKS_API_ENDPOINT + "/taxon_names";
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
                && StringUtils.equals(getVersionSource(statement).toString(), TAXONWORKS_OPEN_PROJECTS.toString())) {
            handleProjectIndex(statement);
        } else if (hasVersionAvailable(statement)
                && StringUtils.startsWith(getVersionSource(statement).getIRIString(), RefNodeFactory.toIRI(TAXONWORKS_CITATIONS).getIRIString())) {
            handleCitations(statement);
        } else if (hasVersionAvailable(statement)
                && StringUtils.startsWith(getVersionSource(statement).getIRIString(), RefNodeFactory.toIRI(TAXONWORKS_BIOLOGICAL_ASSOCIATIONS).getIRIString())) {
            handleBiologicalAssociation(statement);
        } else if (hasVersionAvailable(statement)
                && StringUtils.startsWith(getVersionSource(statement).getIRIString(), RefNodeFactory.toIRI(TAXONWORKS_OTUS).getIRIString())) {
            handleOTUs(statement);
        } else if (hasVersionAvailable(statement)
                && StringUtils.startsWith(getVersionSource(statement).getIRIString(), RefNodeFactory.toIRI(TAXONWORKS_TAXON_NAMES).getIRIString())) {
            handleTaxonNames(statement);
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

    public void handleBiologicalAssociation(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseBiologicalAssociation(currentPage, new StatementsEmitterAdapter() {
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

    public void handleOTUs(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseOTUs(currentPage, new StatementsEmitterAdapter() {
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

    public void handleTaxonNames(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseTaxonName(currentPage, new StatementsEmitterAdapter() {
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

    private void handleProjectIndex(Quad statement) {
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
                parseIndividualCitation(currentPage, emitter, node, versionSource);
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    static void parseBiologicalAssociation(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    parseIndividualBiologicalAssociation(currentPage, emitter, node, versionSource);
                }
            } else {
                parseIndividualBiologicalAssociation(currentPage, emitter, jsonNode, versionSource);
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    static void parseOTUs(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    parseIndividualOTUs(currentPage, emitter, node, versionSource);
                }
            } else {
                parseIndividualOTUs(currentPage, emitter, jsonNode, versionSource);
            }
        }

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    static void parseTaxonName(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        if (jsonNode != null) {
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    parseIndividualTaxonNames(currentPage, emitter, node, versionSource);
                }
            } else {
                parseIndividualTaxonNames(currentPage, emitter, jsonNode, versionSource);
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

    public static void parseIndividualCitation(IRI currentPage, StatementsEmitter emitter, JsonNode result, IRI versionSource) {
        if (result.has("source_id")) {
            String sourceId = result.get("source_id").asText();

            String sourcesUrl = TAXONWORKS_API_ENDPOINT + "/sources/" + sourceId;
            IRI sourceQuery = toIRI(appendProjectTokenIfAvailable(versionSource, sourcesUrl));
            if (result.has("citation_object_id")) {
                emitter.emit(toStatement(currentPage, HAD_MEMBER, sourceQuery));
                emitter.emit(toStatement(sourceQuery, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                emitter.emit(toStatement(sourceQuery, HAS_VERSION, toBlank()));

                String associationId = result.get("citation_object_id").asText();
                String associationsUrl = TAXONWORKS_API_ENDPOINT + "/biological_associations/" + associationId;

                IRI associationQuery = toIRI(appendProjectTokenIfAvailable(versionSource, associationsUrl));
                emitter.emit(toStatement(associationQuery, WAS_DERIVED_FROM, sourceQuery));
                emitter.emit(toStatement(associationQuery, HAS_VERSION, toBlank()));
            }
        }

    }

    private static void parseIndividualBiologicalAssociation(IRI currentPage, StatementsEmitter emitter, JsonNode result, IRI versionSource) {
        if (result.has("biological_association_subject_id")
                && result.has("biological_association_object_id")) {
            Stream<IRI> otuQueries = Stream.of(
                    result.get("biological_association_subject_id").asText(),
                    result.get("biological_association_object_id").asText()
            ).map(id -> TAXONWORKS_API_ENDPOINT + "/otus/" + id)
                    .map(x -> appendProjectTokenIfAvailable(versionSource, x)
                    ).map(RefNodeFactory::toIRI);

            otuQueries.forEach(q -> {
                emitter.emit(toStatement(currentPage, HAD_MEMBER, q));
                emitter.emit(toStatement(q, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                emitter.emit(toStatement(q, HAS_VERSION, toBlank()));

            });
        }

    }

    private static void parseIndividualOTUs(IRI currentPage, StatementsEmitter emitter, JsonNode result, IRI versionSource) {
        if (result.has("taxon_name_id")) {
            Stream<IRI> otuQueries = Stream.of(
                    result.get("taxon_name_id").asText()
            ).map(id -> TAXONWORKS_API_ENDPOINT + "/taxon_names/" + id)
                    .map(x -> appendProjectTokenIfAvailable(versionSource, x)
                    ).map(RefNodeFactory::toIRI);

            otuQueries.forEach(q -> {
                emitter.emit(toStatement(currentPage, HAD_MEMBER, q));
                emitter.emit(toStatement(q, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                emitter.emit(toStatement(q, HAS_VERSION, toBlank()));

            });
        }

    }

    private static void parseIndividualTaxonNames(IRI currentPage, StatementsEmitter emitter, JsonNode result, IRI versionSource) {
        if (result.hasNonNull("parent_id")) {
            Stream<IRI> otuQueries = Stream.of(
                    result.get("parent_id").asText()
            ).map(id -> TAXONWORKS_API_ENDPOINT + "/taxon_names/" + id)
                    .map(x -> appendProjectTokenIfAvailable(versionSource, x)
                    ).map(RefNodeFactory::toIRI);

            otuQueries.forEach(q -> {
                emitter.emit(toStatement(currentPage, HAD_MEMBER, q));
                emitter.emit(toStatement(q, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)));
                emitter.emit(toStatement(q, HAS_VERSION, toBlank()));

            });
        }

    }

    private static String appendProjectTokenIfAvailable(IRI versionSource, String url) {
        Pattern projectToken = Pattern.compile("(.*)([?&]project_token=)(?<projectToken>[a-zA-Z0-9]+)(.*)");
        Matcher matcher = projectToken.matcher(versionSource.getIRIString());
        if (matcher.matches()) {
            url = url + "?project_token=" + matcher.group("projectToken");
        }
        return url;
    }


}
