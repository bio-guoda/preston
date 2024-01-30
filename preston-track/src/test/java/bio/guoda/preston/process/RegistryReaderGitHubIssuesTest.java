package bio.guoda.preston.process;

import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static bio.guoda.preston.RefNodeConstants.HAS_TYPE;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class RegistryReaderGitHubIssuesTest {

    @Test
    public void matchingIssueCommentsAPIURLs() {
        String s = "https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/904/comments";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertTrue(matcher.matches());

        assertThat(matcher.group("repo"), is("globalbioticinteractions"));
        assertThat(matcher.group("org"), is("globalbioticinteractions"));
        assertThat(matcher.group("issueNumber"), is("904"));
    }

    @Test
    public void matchingQueryLatest() {
        String s = "https://api.github.com/repos/globalbioticinteractions/elton/issues?per_page=1&state=all";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertTrue(matcher.matches());

        assertThat(matcher.group("org"), is("globalbioticinteractions"));
        assertThat(matcher.group("repo"), is("elton"));
        assertThat(matcher.group("issueNumber"), is(nullValue()));
    }

    @Test
    public void matchingNonIssueURL() {
        String s = "https://github.com/repos/globalbioticinteractions/nomer";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertFalse(matcher.matches());
    }

    @Test
    public void matchingIssueAPIURLs() {
        String s = "https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/904";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertTrue(matcher.matches());

        assertThat(matcher.group("repo"), is("globalbioticinteractions"));
        assertThat(matcher.group("org"), is("globalbioticinteractions"));
        assertThat(matcher.group("issueNumber"), is("904"));
    }

    @Test
    public void matchingIssueURL() {
        String s = "https://github.com/globalbioticinteractions/globalbioticinteractions/issues/904";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertTrue(matcher.matches());

        assertThat(matcher.group("repo"), is("globalbioticinteractions"));
        assertThat(matcher.group("org"), is("globalbioticinteractions"));
        assertThat(matcher.group("issueNumber"), is("904"));
    }

    @Test
    public void matchingIssuesURLs() {
        String s = "https://github.com/globalbioticinteractions/globalbioticinteractions/issues";

        Matcher matcher = RegistryReaderGitHubIssues.PATTERN_GH_ORG_REPO.matcher(s);

        assertTrue(matcher.matches());

        assertThat(matcher.group("repo"), is("globalbioticinteractions"));
        assertThat(matcher.group("org"), is("globalbioticinteractions"));
        assertThat(matcher.group("issueNumber"), is(nullValue()));
    }

    @Test
    public void onGitHubIssueCommentsAPI() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("904_issue_comments.json");
        ProcessorReadOnly reader = new RegistryReaderGitHubIssues(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/904/comments"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), not(is(0)));
    }


    @Test
    public void onProcessOnce() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("904_issue_comments.json");
        ProcessorReadOnly reader = new RegistryReaderGitHubIssues(blob, TestUtilForProcessor.testListener(nodes));

        reader.on(toStatement(
                toIRI("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/904/comments"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), not(is(0)));

        nodes.clear();
        reader.on(toStatement(
                toIRI("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/904/comments"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onProcessLatest() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("/bio/guoda/preston/process/github/latest_issue.json");
        ProcessorReadOnly reader = new RegistryReaderGitHubIssues(blob, TestUtilForProcessor.testListener(nodes));

        String queryLatest = "https://api.github.com/repos/globalbioticinteractions/elton/issues?per_page=1&state=all";
        reader.on(toStatement(
                toIRI(queryLatest),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(163));

        nodes.clear();
        reader.on(toStatement(
                toIRI(queryLatest),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(nodes.size(), is(0));
    }

    @Test
    public void extractURLs() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/process/github/904_issue_comments.json"));
        List<Pair<URI, URI>> uris = new ArrayList<>();
        RegistryReaderGitHubIssues.appendURIs(jsonNode, uris);
        assertThat(uris,
                containsInAnyOrder(
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/2daeabd5-0e26-41d3-b163-a1b15fe14ea8"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601292454")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/f0cabef8-c85a-495d-b59c-47bec0168e54"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601406328")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/b568da00-123c-4264-8523-1946eef6a6c2"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601582514")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/2eddd4f0-bfea-4522-a3e2-43437c39269e"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601591182")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/765feab5-72f9-4909-87d8-039a651cc6e1"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601591182")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/files/11897856/obj.json.txt"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1611738746")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/files/11898416/genbank-accessions-with-possible-OBI-specimen.tsv.txt"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1611816376")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/files/11822062/OBI-unconventional-catalog-numbers.json.txt"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601360542")),
                        Pair.of(
                                URI.create("https://github.com/globalbioticinteractions/globalbioticinteractions/assets/1084872/505f6b8a-1029-4211-93f0-65b6f826d1b2"),
                                URI.create("https://api.github.com/repos/globalbioticinteractions/globalbioticinteractions/issues/comments/1601300431"))
                ));

    }

    @Test
    public void onLatestIssue() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/process/github/latest_issue.json"));
        List<Quad> statements = new ArrayList<>();
        RegistryReaderGitHubIssues.emitRequestForIssuesUpToMostRecent("foo", "bar", new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        }, jsonNode.get(0));

        assertThat(statements.size(), is(163));

        Quad statement = statements.get(statements.size() - 1);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(SEE_ALSO));
        assertThat(statement.getObject().ntriplesString(), is("<https://github.com/foo/bar/issues/54>"));

        statement = statements.get(statements.size() - 2);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(HAS_VERSION));
        assertThat(statement.getObject().ntriplesString(), startsWith("_:"));

        statement = statements.get(statements.size() - 3);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(HAS_TYPE));
        assertThat(statement.getObject().ntriplesString(), is("\"application/vnd.github+json\""));
    }

    @Test
    public void onGitHubIssueAPI() {
        ArrayList<Quad> statements = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("/bio/guoda/preston/process/github/904_issue.json");
        ProcessorReadOnly reader = new RegistryReaderGitHubIssues(blob, TestUtilForProcessor.testListener(statements));

        reader.on(toStatement(
                toIRI("https://api.github.com/repos/foo/bar/issues/904"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(statements.size(), is(5));


        Quad statement = statements.get(statements.size() - 1);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/904/comments?per_page=100>"));
        assertThat(statement.getPredicate(), is(HAS_VERSION));
        assertThat(statement.getObject().ntriplesString(), startsWith("_:"));


    }

    @Test
    public void onGitHubIssue() {
        ArrayList<Quad> statements = new ArrayList<>();
        BlobStoreReadOnly blob = key -> getClass().getResourceAsStream("/bio/guoda/preston/process/github/904_issue.json");
        ProcessorReadOnly reader = new RegistryReaderGitHubIssues(blob, TestUtilForProcessor.testListener(statements));

        reader.on(toStatement(
                toIRI("https://github.com/repos/foo/bar/issues/54"),
                HAS_VERSION,
                toIRI("http://something")));

        assertThat(statements.size(), not(is(0)));

        Quad statement = statements.get(statements.size() - 1);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(SEE_ALSO));
        assertThat(statement.getObject().ntriplesString(), is("<https://github.com/foo/bar/issues/54>"));

        statement = statements.get(statements.size() - 2);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(HAS_VERSION));
        assertThat(statement.getObject().ntriplesString(), startsWith("_:"));

        statement = statements.get(statements.size() - 3);
        assertThat(statement.getSubject().ntriplesString(), is("<https://api.github.com/repos/foo/bar/issues/54>"));
        assertThat(statement.getPredicate(), is(HAS_TYPE));
        assertThat(statement.getObject().ntriplesString(), is("\"application/vnd.github+json\""));

    }


}