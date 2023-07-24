package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.Image;
import org.commonmark.node.Link;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderGitHubIssues extends ProcessorReadOnly {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryReaderGitHubIssues.class);

    public static final String CHECKLIST_BANK_API_DATASET_PART = "//api.checklistbank.org/dataset";
    public static final String CHECKLIST_BANK_DATASET_REGISTRY_STRING = "https:" + CHECKLIST_BANK_API_DATASET_PART;

    public static final Pattern PATTERN_GH_ORG_REPO = Pattern.compile("http[s]{0,1}://" +
            "([a-zA-Z]+[.]){0,1}" +
            "github[.]com(/repos){0,1}/(?<org>[^/]+)/(?<repo>[^/]+)" +
            "(/(issues(/(?<issueNumber>[0-9]+))){0,1}){0,1}(/.*){0,1}");


    public RegistryReaderGitHubIssues(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            String charSequence = getVersionSource(statement).getIRIString();
            Matcher matcher = PATTERN_GH_ORG_REPO.matcher(charSequence);
            if (matcher.matches()) {
                handleIssues(statement);
            }
        }
    }

    private void handleIssues(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseIssues(currentPage, new StatementsEmitterAdapter() {
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
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "limit=[0-9]*", "limit=" + limit);
        nextPageURL = RegExUtils.replacePattern(nextPageURL, "offset=[0-9]*", "offset=" + offset);
        nextPageURL = StringUtils.contains(nextPageURL, "?") ? nextPageURL : nextPageURL + "?";
        nextPageURL = StringUtils.contains(nextPageURL, "offset") ? nextPageURL : nextPageURL + "&offset=" + offset;
        nextPageURL = StringUtils.contains(nextPageURL, "limit") ? nextPageURL : nextPageURL + "&limit=" + limit;
        nextPageURL = StringUtils.replace(nextPageURL, "?&", "?");
        IRI nextPage = toIRI(nextPageURL);
        emitPageRequest(emitter, nextPage);
    }

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage) {
        Stream.of(
                toStatement(nextPage, CREATED_BY, Seeds.CHECKLIST_BANK),
                toStatement(nextPage, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_JSON)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parseIssues(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(in);
        ArrayList<Pair<URI, URI>> uris = new ArrayList<>();
        appendURIs(jsonNode, uris);

        uris.stream().flatMap(uri -> {
            IRI reference = toIRI(uri.getKey());
            IRI referenceContext = toIRI(uri.getValue());
            return Stream.of(
                    toStatement(versionSource, HAD_MEMBER, reference),
                    toStatement(versionSource, SEE_ALSO, currentPage),
                    toStatement(referenceContext, HAD_MEMBER, reference),
                    toStatement(reference, HAS_VERSION, toBlank())
            );
        }).forEach(emitter::emit);

        emitNextPageIfNeeded(emitter, versionSource, jsonNode);
    }

    public static void appendURIs(JsonNode jsonNode, final List<Pair<URI, URI>> uris) {
        if (jsonNode != null) {
            if (jsonNode.isArray()) {
                for (JsonNode node : jsonNode) {
                    appendReferences(node, uris);
                }
            } else if (jsonNode.isObject()) {
                appendReferences(jsonNode, uris);
            }
        }
    }

    public static void appendReferences(JsonNode node, List<Pair<URI, URI>> referencesInIssueComment) {
        if (node.has("body") && node.has("url")) {
            String url = node.get("url").textValue();
            String body = node.get("body").textValue();
            Parser parser = Parser.builder().build();
            Node document = parser.parse(body);
            document.accept(new AbstractVisitor() {
                @Override
                public void visit(Link link) {
                    appendDestination(link.getDestination(), url, referencesInIssueComment);
                    super.visit(link);
                }

                private void appendDestination(String dest, String context, List<Pair<URI, URI>> uris) {
                    try {
                        uris.add(Pair.of(new URI(dest), new URI(context)));
                    } catch (URISyntaxException e) {
                        // ignore invalid urls
                    }
                }

                @Override
                public void visit(Image link) {
                    appendDestination(link.getDestination(), url, referencesInIssueComment);
                    super.visit(link);
                }
            });
        }
    }

    private static void emitNextPageIfNeeded(StatementsEmitter emitter, IRI versionSource, JsonNode jsonNode) {
        if (!isEndOfRecords(jsonNode)
                && jsonNode.has("total")
                && jsonNode.has("offset")
                && jsonNode.has("limit")) {
            int offset = jsonNode.get("offset").asInt();
            if (offset == 0) {
                int totalNumberOfRecords = jsonNode.get("total").asInt();
                int limit = jsonNode.get("limit").asInt();
                String previousURL = versionSource.getIRIString();
                for (int i = limit; i < totalNumberOfRecords; i += limit) {
                    emitNextPage(i, limit, emitter, previousURL);
                }
            }
        }
    }

    private static boolean isEndOfRecords(JsonNode jsonNode) {
        return false;
    }


    public static void parseIndividualDataset(IRI currentPage, StatementsEmitter emitter, JsonNode result) {
        if (result.has("key")) {
            String datasetId = result.get("key").asText();
            String datasetIRIString = CHECKLIST_BANK_DATASET_REGISTRY_STRING + "/" + datasetId;
            IRI datasetIRI = toIRI(datasetIRIString);
            emitter.emit(toStatement(currentPage, HAD_MEMBER, datasetIRI));
            if (result.has("doi")) {
                String doiString = result.get("doi").asText();
                try {
                    DOI doi = DOI.create(doiString);
                    emitter.emit(toStatement(datasetIRI, SEE_ALSO, toIRI(doi.toURI())));
                } catch (MalformedDOIException e) {
                    LOG.warn("found possibly malformed doi [" + doiString + "]", e);
                }
            }

            if (result.has("gbifKey")) {
                String gbifDatasetString = result.get("gbifKey").asText();
                if (StringUtils.isNotBlank(gbifDatasetString)) {
                    IRI gbifDatasetIRI = RegistryReaderGBIF.GBIFDatasetIRI(gbifDatasetString);
                    emitter.emit(toStatement(datasetIRI, WAS_DERIVED_FROM, gbifDatasetIRI));
                    emitter.emit(toStatement(gbifDatasetIRI, HAS_VERSION, RefNodeFactory.toBlank()));
                }
            }

            IRI datasetArchiveCandidate = toIRI(datasetIRIString + "/archive.zip");
            emitter.emit(toStatement(datasetIRI, HAD_MEMBER, datasetArchiveCandidate));
            emitter.emit(toStatement(datasetArchiveCandidate, HAS_FORMAT, toContentType(MimeTypes.MIME_TYPE_ZIP)));
            emitter.emit(toStatement(datasetArchiveCandidate, HAS_VERSION, toBlank()));
        }
    }

}
