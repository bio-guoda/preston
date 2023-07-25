package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.Image;
import org.commonmark.node.Link;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_TYPE;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderGitHubIssues extends ProcessorReadOnly {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryReaderGitHubIssues.class);

    static final Pattern PATTERN_GH_ORG_REPO = Pattern.compile("http[s]{0,1}://" +
            "([a-zA-Z]+[.]){0,1}" +
            "github[.]com(/repos){0,1}/(?<org>[^/]+)/(?<repo>[^/]+)" +
            "/issues" +
            "(/(?<issueNumber>[0-9]+)){0,1}(/comments){0,1}(\\?.*){0,1}");
    private static final String MOST_RECENT_ISSUE_QUERY = "/issues?per_page=1&state=all";
    private static final String API_PREFIX = "https://api.github.com/repos/";

    private Map<IRI, IRI> processedIRIs = new LRUMap<>(4096);

    public RegistryReaderGitHubIssues(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI versionSourceIRI = getVersionSource(statement);
            if (!processedIRIs.containsKey(versionSourceIRI)) {
                try {
                    processStatement(statement, versionSourceIRI);
                } finally {
                    processedIRIs.put(versionSourceIRI, versionSourceIRI);
                }
            }
        }
    }

    private void processStatement(Quad statement, IRI versionSourceIRI) {
        String versionSource = versionSourceIRI.getIRIString();
        Matcher matcher = PATTERN_GH_ORG_REPO.matcher(versionSource);
        if (matcher.matches()) {
            String org = matcher.group("org");
            String repo = matcher.group("repo");
            if (StringUtils.isBlank(matcher.group("issueNumber"))) {
                if (StringUtils.endsWith(versionSource, MOST_RECENT_ISSUE_QUERY)) {
                    emitIssueRequestsFor(statement, org, repo, this);
                } else {
                    IRI mostRecentRequest = RefNodeFactory.toIRI(API_PREFIX + org + "/" + repo + MOST_RECENT_ISSUE_QUERY);
                    ActivityUtil.emitAsNewActivity(
                            Stream.of(RefNodeFactory.toStatement(mostRecentRequest, HAS_VERSION, toBlank())),
                            this,
                            statement.getGraphName()
                    );
                }
            } else {
                if (StringUtils.startsWith(versionSource, API_PREFIX)) {
                    handleIssues(statement, matcher);
                } else {
                    Stream<Quad> requestIssueComments = createRequestForIssueComments(org, repo, Integer.parseInt(matcher.group("issueNumber")));
                    ActivityUtil.emitAsNewActivity(
                            requestIssueComments,
                            this,
                            statement.getGraphName()
                    );
                }
            }
        }
    }

    private void emitIssueRequestsFor(Quad statement, String org, String repo, StatementsEmitter emitter) {
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                try {
                    JsonNode jsonNode = new ObjectMapper().readTree(is);
                    if (jsonNode != null) {
                        if (jsonNode.isArray()) {
                            for (JsonNode node : jsonNode) {
                                emitRequestForIssueComments(org, repo, emitter, node);
                            }
                        } else if (jsonNode.isObject()) {
                            emitRequestForIssueComments(org, repo, emitter, jsonNode);
                        }
                    }
                } catch (IOException ex) {
                    // ignore malformed json
                }
            }

        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
    }

    public static void emitRequestForIssueComments(String org, String repo, StatementsEmitter emitter, JsonNode node) {
        if (node.has("number")) {
            JsonNode issueNumber = node.get("number");
            if (issueNumber.isInt()) {
                int mostRecentIssue = issueNumber.asInt();
                emitRequestForIssueComments(emitter, org, repo, mostRecentIssue);
            }
        }
    }

    private static void emitRequestForIssueComments(StatementsEmitter emitter, String org, String repo, int mostRecentIssue) {
        Stream<Quad> statements = IntStream
                .rangeClosed(1, mostRecentIssue)
                .mapToObj(issue -> createRequestForIssueComments(org, repo, issue))
                .flatMap(Function.identity());
        ActivityUtil.emitAsNewActivity(statements, emitter, Optional.empty());
    }

    private static Stream<Quad> createRequestForIssueComments(String org, String repo, int issue) {
        String issueSuffix = org + "/" + repo + "/issues/" + issue;
        String issueRequestPrefix = API_PREFIX + issueSuffix;
        IRI issueRequest = toIRI(issueRequestPrefix);
        IRI issueCommentsRequest = toIRI(issueRequestPrefix + "/comments");
        return Stream.of(
                toStatement(issueRequest, HAS_TYPE, RefNodeFactory.toLiteral(ResourcesHTTP.MIMETYPE_GITHUB_JSON)),
                toStatement(issueRequest, HAS_VERSION, toBlank()),
                toStatement(issueRequest, SEE_ALSO, toIRI("https://github.com/" + issueSuffix)),
                toStatement(issueRequest, HAD_MEMBER, issueCommentsRequest),
                toStatement(issueCommentsRequest, HAS_TYPE, RefNodeFactory.toLiteral(ResourcesHTTP.MIMETYPE_GITHUB_JSON)),
                toStatement(issueCommentsRequest, HAS_VERSION, toBlank())
        );
    }

    private void handleIssues(Quad statement, Matcher matcher) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseIssuesIgnoreUnexpected(currentPage, new StatementsEmitterAdapter() {
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

    private static void parseIssuesIgnoreUnexpected(
            IRI currentPage,
            StatementsEmitter emitter,
            InputStream in,
            IRI versionSource) {
        try {
            JsonNode jsonNode = new ObjectMapper().readTree(in);
            ArrayList<Pair<URI, URI>> uris = new ArrayList<>();
            appendURIs(jsonNode, uris);

            uris.stream().flatMap(uri -> {
                IRI destination = toIRI(uri.getKey());
                IRI issueContext = toIRI(uri.getValue());
                return Stream.of(
                        toStatement(currentPage, HAD_MEMBER, destination),
                        toStatement(issueContext, HAD_MEMBER, destination),
                        toStatement(destination, HAS_VERSION, toBlank())
                );
            }).forEach(emitter::emit);

        } catch (IOException ex) {
            // ignore malformed json
        }
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

    private static boolean isEndOfRecords(JsonNode jsonNode) {
        return false;
    }


}
