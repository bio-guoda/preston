package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import bio.guoda.preston.stream.MatchingTextStreamHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForTextResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

public class TextMatcherTest {

    @Test
    public void onUnresolvable() {
        BlobStoreReadOnly blobStore = TestUtil.getTestBlobStore(HashType.sha256);

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));

        assertThat(nodes.get(2).toString(), containsString("<http://purl.org/dc/terms/description> \"An activity"));

    }

    @Test
    public void onNonparseable() {
        BlobStoreReadOnly blobStore = key -> new ByteArrayInputStream(new byte[0]);

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onTextWithNoUrls() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("haha no URLs to see here", StandardCharsets.UTF_8);

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/plazidwca.zip");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(153));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/meta.xml!/b56-83> <http://www.w3.org/ns/prov#value> \"http://rs.tdwg.org/dwc/text/\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/media.txt!/b15496-15557> <http://www.w3.org/ns/prov#value> \"http://treatment.plazi.org/id/D51D87C0FFC3C7624B9C5739FC6EDCBF\""));
    }

    @Test
    public void onZipBatchSize100() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/plazidwca.zip");

        ArrayList<Quad> nodes = runTextFinder(blobStore, 100);
        assertThat(nodes.size(), is(156));

        long numberOfActivities = nodes.stream()
                .map(Object::toString)
                .filter(x -> StringUtils.contains(x, RefNodeConstants.ACTIVITY.getIRIString()))
                .count();

        assertThat(numberOfActivities, is(2L));
    }

    @Test
    public void onText() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForTextResource("/bio/guoda/preston/process/bhl_item.txt");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(12));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<cut:hash://sha256/blub!/b191-233> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:hash://sha256/blub!/b1670-1713> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\""));
    }

    @Test
    public void findMatchesInLines() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForTextResource("/bio/guoda/preston/process/bhl_item.txt");

        ArrayList<Quad> nodes = new TextFinder(blobStore).separateLines(true).findMatches();
        assertThat(nodes.size(), is(12));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<cut:line:hash://sha256/blub!/L2!/b59-101> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:line:hash://sha256/blub!/L10!/b66-109> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\""));
    }

    @Test
    public void findMatchingLines() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForTextResource("/bio/guoda/preston/process/bhl_item.txt");

        ArrayList<Quad> nodes = new TextFinder(blobStore).reportOnlyMatchingText(false).separateLines(true).findMatches();
        assertThat(nodes.size(), is(12));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<line:hash://sha256/blub!/L2> <http://www.w3.org/ns/prov#value> \"24\t11\t268274\tmobot31753000022803\ti11499722\tQK98 .R6 1789\t\thttps://www.biodiversitylibrary.org/item/24 \t\t1789\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<line:hash://sha256/blub!/L10> <http://www.w3.org/ns/prov#value> \"947\t64\t44519\tmobot31753002306964\ti11595310\tQK1 .F418\tv.33 (1850)\thttps://www.biodiversitylibrary.org/item/947 \t\t1850\tMissouri Botanical Garden, Peter H. Raven Library\t\t2006-05-04 00:00\""));
    }

    @Test
    public void onNestedZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.zip");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(5));

        String level1UrlStatement = nodes.get(3).toString();
        assertThat(level1UrlStatement, startsWith("<cut:zip:zip:hash://sha256/blub!/level2.zip!/level2.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));

        String level2UrlStatement = nodes.get(4).toString();
        assertThat(level2UrlStatement, startsWith("<cut:zip:hash://sha256/blub!/level1.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }

    @Test
    public void onNestedTarGz() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.tar.gz");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(5));

        String level1UrlStatement = nodes.get(3).toString();
        assertThat(level1UrlStatement, startsWith("<cut:tar:gz:hash://sha256/blub!/level1.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));

        String level2UrlStatement = nodes.get(4).toString();
        assertThat(level2UrlStatement, startsWith("<cut:zip:tar:gz:hash://sha256/blub!/level2.zip!/level2.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }

    @Test
    public void onArchiveWithWhitespaceInFileNames() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/archive-with-whitespace-file-names.zip");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(4));

        String level1UrlStatement = nodes.get(3).toString();
        assertThat(level1UrlStatement, startsWith("<cut:zip:hash://sha256/blub!/name%20with%20spaces.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }

    @Test
    public void parseFileWithNoEarlyMatches() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/data/97/cb/97cbeae429fbc95d1859f7afa28b33f08ac64125ba72511c49c4b77ca66d2d66");
        runTextFinder(blobStore);
    }

    @Test
    public void parseFileWithMalformedCharacters() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/data/42/56/4256fd83db9270d2236776bc4bd45e22b15235e0798ba59952f8ddd1f7fbe7b2");
        runTextFinder(blobStore);
    }

    @Test
    public void findMultiLineMatches() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("one\ntwo\nthree", StandardCharsets.UTF_8);

        ArrayList<Quad> nodes = runTextFinder(blobStore, Pattern.compile("one\ntwo"));
        assertThat(nodes.size(), is(4));
    }

    @Test
    public void detectMatchesAcrossBufferBoundaries() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForTextResource("/bio/guoda/preston/process/textmatcher-4KB-boundary-test.tsv");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(4));

        String matchAcrossBufferBoundary = nodes.get(nodes.size() - 1).toString();
        assertThat(matchAcrossBufferBoundary, startsWith("<cut:hash://sha256/blub!/b4081-4115> <http://www.w3.org/ns/prov#value> \"http://thereisa.4KBboundaryhere.com\""));
    }

    @Test
    public void detectSymbiotaOccurrenceReferences() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForTextResource("/bio/guoda/preston/process/textmatcher-symbiota-test.tsv");

        ArrayList<Quad> nodes = runTextFinder(blobStore, Pattern.compile("http[s]{0,1}://[a-zA-Z0-9.]+/portal/collections/individual/index.php[?]{1}occid=[0-9]+"));

        String matchAcrossBufferBoundary = nodes.get(nodes.size() - 1).toString();
        assertThat(matchAcrossBufferBoundary, startsWith("<cut:hash://sha256/blub!/b47-123> <http://www.w3.org/ns/prov#value> \"http://openherbarium.org/portal/collections/individual/index.php?occid=157755\""));
    }

    @Test
    public void usingRegexGroups() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("the duck is in the pond", StandardCharsets.UTF_8);

        ArrayList<Quad> nodes = runTextFinder(blobStore, Pattern.compile("(?:the) (?<what>\\S+) (?>is) in the (\\S+)"));
        assertThat(nodes.size(), is(9));

        List<String> triples = nodes.stream().map((quad) -> quad.asTriple().toString()).collect(Collectors.toList());

        assertTrue(triples.contains("<cut:hash://sha256/blub!/b1-23> <http://www.w3.org/ns/prov#value> \"the duck is in the pond\" ."));
        assertTrue(triples.contains("<cut:hash://sha256/blub!/b5-8> <http://www.w3.org/ns/prov#value> \"duck\" ."));
        assertTrue(triples.contains("<cut:hash://sha256/blub!/b1-23> <http://www.w3.org/ns/prov#hadMember> <cut:hash://sha256/blub!/b5-8> ."));
        assertTrue(triples.contains("<cut:hash://sha256/blub!/b1-23> <http://purl.org/dc/terms/description> \"what\" ."));
        assertTrue(triples.contains("<cut:hash://sha256/blub!/b20-23> <http://www.w3.org/ns/prov#value> \"pond\" ."));
        assertTrue(triples.contains("<cut:hash://sha256/blub!/b1-23> <http://www.w3.org/ns/prov#hadMember> <cut:hash://sha256/blub!/b20-23> ."));
    }

    @Test
    public void withMaxNumMatches() {
        final int numActivityLines = 3;
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.zip");

        ArrayList<Quad> nodes = runTextFinder(blobStore);
        assertThat(nodes.size(), is(numActivityLines + 2));

        nodes = runTextFinder(blobStore, TextMatcher.URL_PATTERN, 256, 1);
        assertThat(nodes.size(), is(numActivityLines + 1));
    }

    @Test
    public void withMaxNumMatchesAndReportingMoreThanMatch() {
        final int numActivityLines = 3;
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.zip");

        ArrayList<Quad> nodes = new TextFinder(blobStore)
                .maxNumMatches(1)
                .reportOnlyMatchingText(false)
                .separateLines(true)
                .findMatches();

        assertThat(nodes.size(), is(numActivityLines + 1));
    }

    private class TextFinder {
        private final KeyValueStoreReadOnly blobStore;
        private Pattern pattern;
        private int batchSize;
        private int maxNumMatches;
        private boolean reportOnlyMatchingText;
        private boolean separateLines;

        public TextFinder(KeyValueStoreReadOnly blobStore) {
            this.blobStore = blobStore;
            this.pattern = TextMatcher.URL_PATTERN;
            this.batchSize = 256;
            this.maxNumMatches = 0;
            this.reportOnlyMatchingText = true;
            this.separateLines = false;
        }

        public TextFinder pattern(Pattern pattern) {
            this.pattern = pattern;
            return this;
        }

        public TextFinder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public TextFinder maxNumMatches(int maxNumMatches) {
            this.maxNumMatches = maxNumMatches;
            return this;
        }

        public TextFinder reportOnlyMatchingText(boolean reportOnlyMatchingText) {
            this.reportOnlyMatchingText = reportOnlyMatchingText;
            return this;
        }

        public TextFinder separateLines(boolean separateLines) {
            this.separateLines = separateLines;
            return this;
        }

        public ArrayList<Quad> findMatches() {
            ArrayList<Quad> nodes = new ArrayList<>();
            TextMatcher textMatcher = new TextMatcher(
                    pattern,
                    maxNumMatches,
                    reportOnlyMatchingText,
                    separateLines,
                    new ProcessorStateAlwaysContinue(),
                    blobStore,
                    TestUtilForProcessor.testListener(nodes)
            );
            textMatcher.setBatchSize(batchSize);

            Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

            textMatcher.on(statement);
            return nodes;
        }
    }
    private ArrayList<Quad> runTextFinder(BlobStoreReadOnly blobStore) { return runTextFinder(blobStore, TextMatcher.URL_PATTERN, 256, 0); }

    private ArrayList<Quad> runTextFinder(BlobStoreReadOnly blobStore, int batchSize) { return runTextFinder(blobStore, TextMatcher.URL_PATTERN, batchSize, 0); }

    private ArrayList<Quad> runTextFinder(BlobStoreReadOnly blobStore, Pattern pattern) { return runTextFinder(blobStore, pattern, 256, 0); }

    private ArrayList<Quad> runTextFinder(BlobStoreReadOnly blobStore, Pattern pattern, int batchSize, int maxNumMatches) {
        return new TextFinder(blobStore)
                .pattern(pattern)
                .batchSize(batchSize)
                .maxNumMatches(maxNumMatches)
                .findMatches();
    }

    @Test
    public void extractPatternGroupNames() {
        Pattern pattern = Pattern.compile("(no)(?>no)(?<group1>yes[^(]no[)]_[(no)[(no)]]_[(no[)]]_(?:no(?<group2>yes)))");
        Map<Integer, String> groupNames = MatchingTextStreamHandler.extractPatternGroupNames(pattern);

        assertThat(groupNames.size(), is(2));
        assertThat(groupNames.get(2), is("group1"));
        assertThat(groupNames.get(3), is("group2"));
    }

    @Test
    public void sterilizeEscapes() {
        Pattern pattern = Pattern.compile("\\(blah\\)");
        Pattern sterilizedPattern = MatchingTextStreamHandler.sterilizePatternForGroupDetection(pattern);
        assertThat(sterilizedPattern.pattern(), is(".blah."));
    }

    @Test
    public void sterilizeClasses() {
        Pattern pattern = Pattern.compile("[blah]");
        Pattern sterilizedPattern = MatchingTextStreamHandler.sterilizePatternForGroupDetection(pattern);
        assertThat(sterilizedPattern.pattern(), is("."));
    }

    @Test
    public void sterilizeSomethingMessy() {
        Pattern pattern = Pattern.compile("(w)(?>w)(?<g1>w[^(]w[)]_[(w)[(w)]]_[(w[)]]_(?:w(?<g2>w)))");
        Pattern sterilizedPattern = MatchingTextStreamHandler.sterilizePatternForGroupDetection(pattern);
        assertThat(sterilizedPattern.pattern(), is("(w)(?>w)(?<g1>w.w._._._(?:w(?<g2>w)))"));
    }
}
