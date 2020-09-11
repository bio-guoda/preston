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
        URLFinder zipReader = new URLFinder(blobStore, TestUtil.testListener(nodes));

        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/blub"));

        zipReader.on(statement);

        assertThat(nodes.size(), is(151));

        String firstUrlStatement = nodes.get(1).toString();
        assertThat(firstUrlStatement, startsWith("<http://rs.tdwg.org/dwc/text/> <locatedAt> <zip:hash://sha256/blub!/meta.xml#L2>"));

        String lastUrlStatement = nodes.get(nodes.size() - 1).toString();
        assertThat(lastUrlStatement, startsWith("<http://treatment.plazi.org/id/D51D87C0FFC3C7624B9C5739FC6EDCBF> <locatedAt> <zip:hash://sha256/blub!/media.txt#L3>"));
    }

    @Test
    public void onText() {
    }
}
