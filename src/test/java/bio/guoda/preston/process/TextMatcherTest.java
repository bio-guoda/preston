package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.store.TestUtil;
import com.mchange.v1.io.InputStreamUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TextMatcherTest {

    @Test
    public void onUnresolvable() {
        BlobStoreReadOnly blobStore = TestUtil.getTestBlobStore();

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));

        assertThat(nodes.get(2).toString(), containsString("<http://purl.org/dc/terms/description> \"An activity"));

    }

    @Test
    public void onNonparseable() {
        BlobStoreReadOnly blobStore = key -> InputStreamUtils.getEmptyInputStream();

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onTextWithNoUrls() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("haha no URLs to see here", Charset.defaultCharset());

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(3));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/plazidwca.zip");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(153));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/meta.xml!/b56-83> <http://www.w3.org/ns/prov#value> \"http://rs.tdwg.org/dwc/text/\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/media.txt!/b15496-15557> <http://www.w3.org/ns/prov#value> \"http://treatment.plazi.org/id/D51D87C0FFC3C7624B9C5739FC6EDCBF\""));
    }

    @Test
    public void onZipBatchSize100() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/plazidwca.zip");

        ArrayList<Quad> nodes = runUrlFinder(blobStore, 100);
        assertThat(nodes.size(), is(156));

        long numberOfActivities = nodes.stream()
                .map(Object::toString)
                .filter(x -> StringUtils.contains(x, RefNodeConstants.ACTIVITY.getIRIString()))
                .count();

        assertThat(numberOfActivities, is(2L));
    }

    @Test
    public void onText() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/bhl_item.txt");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(12));

        String firstUrlStatement = nodes.get(3).toString();
        assertThat(firstUrlStatement, startsWith("<cut:hash://sha256/blub!/b191-233> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:hash://sha256/blub!/b1670-1713> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\""));
    }

    @Test
    public void onNestedZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.zip");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(5));

        String level1UrlStatement = nodes.get(3).toString();
        assertThat(level1UrlStatement, startsWith("<cut:zip:zip:hash://sha256/blub!/level2.zip!/level2.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));

        String level2UrlStatement = nodes.get(4).toString();
        assertThat(level2UrlStatement, startsWith("<cut:zip:hash://sha256/blub!/level1.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }

    @Test
    public void parseFileWithNoEarlyMatches() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/data/97/cb/97cbeae429fbc95d1859f7afa28b33f08ac64125ba72511c49c4b77ca66d2d66");
        runUrlFinder(blobStore);
    }

    @Test
    public void parseFileWithMalformedCharacters() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/data/42/56/4256fd83db9270d2236776bc4bd45e22b15235e0798ba59952f8ddd1f7fbe7b2");
        runUrlFinder(blobStore);
    }

    @Test
    public void detectMatchesAcrossBufferBoundaries() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/textmatcher-4KB-boundary-test.tsv");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(4));

        String matchAcrossBufferBoundary = nodes.get(nodes.size() - 1).toString();
        assertThat(matchAcrossBufferBoundary, startsWith("<cut:hash://sha256/blub!/b4081-4114> <http://www.w3.org/ns/prov#value> \"http://thereisa4KBboundaryhere.com\""));
    }

    private ArrayList<Quad> runUrlFinder(BlobStoreReadOnly blobStore) {
        return runUrlFinder(blobStore, 256);
    }

    private ArrayList<Quad> runUrlFinder(BlobStoreReadOnly blobStore, int batchSize) {
        ArrayList<Quad> nodes = new ArrayList<>();
        TextMatcher textMatcher = new TextMatcher(blobStore, TestUtil.testListener(nodes));
        textMatcher.setBatchSize(batchSize);

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        textMatcher.on(statement);
        return nodes;
    }
}
