package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Optional;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
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

    private Queue<IRI> processedIRIs = new CircularFifoQueue<IRI>(2048);

    public RegistryReaderGitHubIssues(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            IRI versionSourceIRI = getVersionSource(statement);
            if (!processedIRIs.contains(versionSourceIRI)) {
                try {
                    processStatement(statement, versionSourceIRI);
                } finally {
                    processedIRIs.add(versionSourceIRI);
                }
            }
        }
    }

    private void processStatement(Quad statement, IRI versionSourceIRI) {
        String versionSource = versionSourceIRI.getIRIString();
        Matcher matcher = PATTERN_GH_ORG_REPO.matcher(versionSource);
        if (matcher.matches()) {
            if (StringUtils.isBlank(matcher.group("issueNumber"))) {
                String org = matcher.group("org");
                String repo = matcher.group("repo");
                if (StringUtils.endsWith(versionSource, MOST_RECENT_ISSUE_QUERY)) {
                    emitIssueRequestsFor(statement, org, repo, this);
                } else {
                    IRI mostRecentRequest = RefNodeFactory.toIRI("https://api.github.com/repos/" + org + "/" + repo + MOST_RECENT_ISSUE_QUERY);
                    ActivityUtil.emitAsNewActivity(
                            Stream.of(RefNodeFactory.toStatement(mostRecentRequest, HAS_VERSION, toBlank())),
                            this,
                            statement.getGraphName()
                    );
                }
            } else {
                handleIssues(statement);
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
                Stream<Quad> statements = IntStream
                        .rangeClosed(1, mostRecentIssue)
                        .mapToObj(issue -> {
                            IRI issueCommentsRequest = toIRI(API_PREFIX + org + "/" + repo + "/issues/" + issue + "/comments");
                            return toStatement(issueCommentsRequest, HAS_VERSION, toBlank());
                        });
                ActivityUtil.emitAsNewActivity(statements, emitter, Optional.empty());
            }
        }
    }

    private void handleIssues(Quad statement) {
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

    private static void parseIssuesIgnoreUnexpected(IRI currentPage, StatementsEmitter emitter, InputStream in, IRI
            versionSource) throws IOException {
        try {
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
