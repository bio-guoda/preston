package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class RegistryReaderDOITest {

    @Test
    public void onGBIFDoiNoPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderDOI registryReader = new RegistryReaderDOI(TestUtil.getTestBlobStore(), nodes::add);
        registryReader.on(toStatement(toIRI("https://doi.org/10.15468/dl.4n9w6m"), HAS_VERSION, toIRI("some://hash")));

        assertThat(new HashSet<>(nodes).size(), is(1));
        assertThat(nodes.get(0).toString(), is("https://api.gbif.org/v1/dataset"));

        Assert.assertThat(nodes.size(), is(6));
    }

    @Test
    public void parseGBIFDownloadPage() throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
       StatementEmitter emitter = nodes::add;

        Quad versionStatement = toStatement(toIRI(
                "https://doi.org/10.15468/dl.4n9w6m"),
                HAS_VERSION,
                toIRI("some://hash"));

        RegistryReaderDOI.parseGBIFDownloadHtmlPage(versionStatement, getClass().getResourceAsStream(
                "/bio/guoda/preston/process/gbif-download-page.html"),
                emitter);

        assertThat(new HashSet<>(nodes).size(), is(2));
        assertThat(nodes.get(1).toString(), startsWith("<https://api.gbif.org/v1/occurrence/download/0062961-200221144449610> <http://purl.org/pav/hasVersion> "));
    }


}