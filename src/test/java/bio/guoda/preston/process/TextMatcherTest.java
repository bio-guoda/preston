package bio.guoda.preston.process;

import bio.guoda.preston.cmd.Persisting;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystemTest;
import bio.guoda.preston.store.TestUtil;
import com.mchange.v1.io.InputStreamUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static bio.guoda.preston.store.TestUtil.getTestBlobStoreForResource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class TextMatcherTest {

    @Test
    public void onUnresolvable() {
        BlobStoreReadOnly blobStore = TestUtil.getTestBlobStore();

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(2));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onNonparseable() {
        BlobStoreReadOnly blobStore = key -> InputStreamUtils.getEmptyInputStream();

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(2));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onTextWithNoUrls() {
        BlobStoreReadOnly blobStore = key -> IOUtils.toInputStream("haha no URLs to see here", Charset.defaultCharset());

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(2));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blub>"));
    }

    @Test
    public void onZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/plazidwca.zip");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(152));

        String firstUrlStatement = nodes.get(2).toString();
        assertThat(firstUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/meta.xml!/b56-83> <http://www.w3.org/ns/prov#value> \"http://rs.tdwg.org/dwc/text/\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/media.txt!/b15496-15557> <http://www.w3.org/ns/prov#value> \"http://treatment.plazi.org/id/D51D87C0FFC3C7624B9C5739FC6EDCBF\""));
    }

    @Test
    public void onText() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/bhl_item.txt");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(11));

        String firstUrlStatement = nodes.get(2).toString();
        assertThat(firstUrlStatement, startsWith("<cut:hash://sha256/blub!/b191-233> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:hash://sha256/blub!/b1670-1713> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\""));
    }

    @Test
    public void onNestedZip() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/process/nested.zip");

        ArrayList<Quad> nodes = runUrlFinder(blobStore);
        assertThat(nodes.size(), is(4));

        String level1UrlStatement = nodes.get(2).toString();
        assertThat(level1UrlStatement, startsWith("<cut:zip:zip:hash://sha256/blub!/level2.zip!/level2.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));

        String level2UrlStatement = nodes.get(3).toString();
        assertThat(level2UrlStatement, startsWith("<cut:zip:hash://sha256/blub!/level1.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }

    @Test
    public void noArrayIndexOutOfBoundsException() {
        BlobStoreReadOnly blobStore = getTestBlobStoreForResource("/bio/guoda/preston/data/97/cb/97cbeae429fbc95d1859f7afa28b33f08ac64125ba72511c49c4b77ca66d2d66");
        runUrlFinder(blobStore);
    }

    private ArrayList<Quad> runUrlFinder(BlobStoreReadOnly blobStore) {
        ArrayList<Quad> nodes = new ArrayList<>();
        TextMatcher textMatcher = new TextMatcher(blobStore, TestUtil.testListener(nodes));

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        textMatcher.on(statement);
        return nodes;
    }
}
