package bio.guoda.preston.process;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
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
    public void onGBIFDoiWithLandingPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                if (!StringUtils.equals(key.getIRIString(),
                        toIRI("some://hash").getIRIString())) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream("gbif-download-page.html");
            }
        };

        RegistryReaderDOI registryReader = new RegistryReaderDOI(blobStore, TestUtil.testListener(nodes));
        registryReader.on(toStatement(toIRI("https://doi.org/10.15468/dl.4n9w6m"), HAS_VERSION, toIRI("some://hash")));

        assertThat(new HashSet<>(nodes).size(), is(2));
        assertThat(nodes.get(1).toString(), startsWith("<https://api.gbif.org/v1/occurrence/download/0062961-200221144449610> <http://purl.org/pav/hasVersion> "));

    }

    @Test
    public void onNonGBIFDoi() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = key -> {
            throw new IOException("kaboom!");
        };

        RegistryReaderDOI registryReader = new RegistryReaderDOI(blobStore, TestUtil.testListener(nodes));
        registryReader.on(toStatement(toIRI("https://doi.org/10.1234/dl.4n9w6m"), HAS_VERSION, toIRI("some://hash")));

        assertThat(new HashSet<>(nodes).size(), is(0));

    }

    @Test
    public void onGBIFDoiNoPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = key -> {
            throw new IOException("kaboom!");
        };

        RegistryReaderDOI registryReader = new RegistryReaderDOI(blobStore, TestUtil.testListener(nodes));
        registryReader.on(toStatement(toIRI("https://doi.org/10.15468/dl.4n9w6m"), HAS_VERSION, toIRI("some://hash")));

        assertThat(new HashSet<>(nodes).size(), is(0));

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