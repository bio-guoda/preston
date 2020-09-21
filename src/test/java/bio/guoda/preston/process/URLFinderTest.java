package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class URLFinderTest {

    @Test
    public void onZip() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream("/bio/guoda/preston/plazidwca.zip");
            }
        };

        ArrayList<Quad> nodes = new ArrayList<>();
        URLFinder urlFinder = new URLFinder(blobStore, TestUtil.testListener(nodes));

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        urlFinder.on(statement);

        assertThat(nodes.size(), is(151));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/meta.xml!/b56-83> <http://www.w3.org/ns/prov#value> \"http://rs.tdwg.org/dwc/text/\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:zip:hash://sha256/blub!/media.txt!/b15496-15557> <http://www.w3.org/ns/prov#value> \"http://treatment.plazi.org/id/D51D87C0FFC3C7624B9C5739FC6EDCBF\""));
    }

    @Test
    public void onText() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream("/bio/guoda/preston/process/bhl_item.txt");
            }
        };

        ArrayList<Quad> nodes = new ArrayList<>();
        URLFinder urlFinder = new URLFinder(blobStore, TestUtil.testListener(nodes));

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        urlFinder.on(statement);

        assertThat(nodes.size(), is(10));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, startsWith("<cut:hash://sha256/blub!/b191-233> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/24\""));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<cut:hash://sha256/blub!/b1670-1713> <http://www.w3.org/ns/prov#value> \"https://www.biodiversitylibrary.org/item/947\""));
    }

    @Test
    public void onNestedZip() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream("/bio/guoda/preston/process/nested.zip");
            }
        };

        ArrayList<Quad> nodes = new ArrayList<>();
        URLFinder urlFinder = new URLFinder(blobStore, TestUtil.testListener(nodes));

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        urlFinder.on(statement);

        assertThat(nodes.size(), is(3));

        String level1UrlStatement = nodes.get(1).toString();
        assertThat(level1UrlStatement, startsWith("<cut:zip:zip:hash://sha256/blub!/level2.zip!/level2.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));

        String level2UrlStatement = nodes.get(2).toString();
        assertThat(level2UrlStatement, startsWith("<cut:zip:hash://sha256/blub!/level1.txt!/b1-19> <http://www.w3.org/ns/prov#value> \"https://example.org\""));
    }
}
