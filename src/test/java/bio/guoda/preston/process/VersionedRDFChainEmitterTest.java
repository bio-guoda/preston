package bio.guoda.preston.process;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

public class VersionedRDFChainEmitterTest {

    public static final IRI SOME = RefNodeFactory.toIRI("http://some");
    public static final IRI OTHER = RefNodeFactory.toIRI("http://other");
    @Test
    public void replayArchive() {
        List<Triple> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, nodes::add);
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.ARCHIVE, RefNodeConstants.HAS_VERSION, SOME));

        assertThat(nodes.size(), Is.is(10));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(3).getSubject(), Is.is(RefNodeFactory.toIRI("5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1")));
    }

    private BlobStoreReadOnly createBlobStore() {
        return new BlobStoreReadOnly() {
                @Override
                public InputStream get(IRI key) throws IOException {
                    if (key.equals(SOME)) {
                        return getClass().getResourceAsStream("archivetest.nq");
                    } else if (key.equals(OTHER)) {
                        return getClass().getResourceAsStream("archivetest2.nq");
                    } else {
                        return null;
                    }
                }
            };
    }

    @Test
    public void replayArchiveMultipleVersions() {
        List<Triple> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, nodes::add);
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.ARCHIVE, RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("http://some")));
        reader.on(RefNodeFactory
                .toStatement(OTHER, RefNodeConstants.HAS_PREVIOUS_VERSION, RefNodeFactory.toIRI("http://some")));

        assertThat(nodes.size(), Is.is(20));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(3).getSubject(), Is.is(RefNodeFactory.toIRI("5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1")));
        assertThat(nodes.get(10).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(13).getSubject(), Is.is(RefNodeFactory.toIRI("653d230a-4cdc-46da-99b6-3df33fb48a55")));

    }


    @Test
    public void invalidDateTime() {
        String nqString = "<hash://sha256/47915f03b8ed8469ecdb2a2c44798aac94b8156d7f3fdfc4b27db02aea6e0faf> <http://www.w3.org/ns/prov#hadMember> <https://invertnet.org/idigbio-feed/datasets/purc.zip> .\n" +
                "<https://invertnet.org/idigbio-feed/datasets/purc.zip> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" .\n" +
                "<hash://sha256/73257a4e2256d17195db2053d64983bb44f90a38b43fd74e5c746a146ef476a1> <http://www.w3.org/ns/prov#generatedAtTime> \"2018-09-05T07:36:31.735Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n" +
                "<hash://sha256/73257a4e2256d17195db2053d64983bb44f90a38b43fd74e5c746a146ef476a1> <http://www.w3.org/ns/prov#wasGeneratedBy> <90fa570e-579f-4e98-b060-f5886019f0b6> .\n" +
                "<https://invertnet.org/idigbio-feed/datasets/purc.zip> <http://purl.org/pav/hasVersion> <hash://sha256/73257a4e2256d17195db2053d64983bb44f90a38b43fd74e5c746a146ef476a1> .\n" +
                "<hash://sha256/47915f03b8ed8469ecdb2a2c44798aac94b8156d7f3fdfc4b27db02aea6e0faf> <http://www.w3.org/ns/prov#generatedAtTime> \"2018-09-05T07:29:28.118Z\" .\n" +
                "<hash://sha256/47915f03b8ed8469ecdb2a2c44798aac94b8156d7f3fdfc4b27db02aea6e0faf> <http://www.w3.org/ns/prov#hadMember> <https://invertnet.org/idigbio-feed/datasets/inhs.zip> .\n" +
                "<https://invertnet.org/idigbio-feed/datasets/inhs.zip> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" .\n" +
                "<hash://sha256/2e935efafe03a7c398492a9aa817842daf513630e4c706be0f2f09cf42b745ce> <http://www.w3.org/ns/prov#generatedAtTime> \"2018-09-05T07:36:32.662Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n" +
                "<hash://sha256/2e935efafe03a7c398492a9aa817842daf513630e4c706be0f2f09cf42b745ce> <http://www.w3.org/ns/prov#wasGeneratedBy> <90fa570e-579f-4e98-b060-f5886019f0b6> .\n" +
                "<https://invertnet.org/idigbio-feed/datasets/inhs.zip> <http://purl.org/pav/hasVersion> <hash://sha256/2e935efafe03a7c398492a9aa817842daf513630e4c706be0f2f09cf42b745ce> .\n" +
                "<hash://sha256/47915f03b8ed8469ecdb2a2c44798aac94b8156d7f3fdfc4b27db02aea6e0faf> <http://www.w3.org/ns/prov#generatedAtTime> \"2018-09-05T07:29:28.118Z\" .\n" +
                "<hash://sha256/47915f03b8ed8469ecdb2a2c44798aac94b8156d7f3fdfc4b27db02aea6e0faf> <http://www.w3.org/ns/prov#hadMember> <https://invertnet.org/idigbio-feed/datasets/ku.zip> .\n";

        BlobStoreReadOnly testStore = new BlobStoreReadOnly() {

            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("bla");
            }
        };
        final StringBuilder actual = new StringBuilder();
        new VersionedRDFChainEmitter(testStore, new StatementListener() {
            @Override
            public void on(Triple statement) {
                actual.append(statement.toString());
                actual.append("\n");
            }
        }).parseAndEmit(IOUtils.toInputStream(nqString, StandardCharsets.UTF_8));

        assertThat(actual.toString(), Is.is(nqString));
    }

}