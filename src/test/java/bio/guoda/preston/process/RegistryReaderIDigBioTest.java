package bio.guoda.preston.process;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RegistryReaderIDigBioTest {

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(), nodes::add);
        RDFTerm bla = RefNodeFactory.toLiteral("bla");
        reader.on(RefNodeFactory.toStatement(Seeds.IDIGBIO, WAS_ASSOCIATED_WITH, bla));
        assertThat(nodes.size(), is(6));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(TestUtil.getTestBlobStore(), nodes::add);
        RDFTerm bla = RefNodeFactory.toLiteral("bla");
        reader.on(RefNodeFactory.toStatement(Seeds.IDIGBIO, RefNodeFactory.toIRI("https://example.org/bla"), bla));
        assertThat(nodes.size(), is(0));
    }


    @Test
    public void onRegistry() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStore blob = new BlobStore() {

            @Override
            public IRI put(InputStream is) throws IOException {
                return null;
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return publishersInputStream();
            }
        };
        RegistryReaderIDigBio reader = new RegistryReaderIDigBio(blob, nodes::add);
        reader.on(RefNodeFactory.toStatement(RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/publishers"), HAS_VERSION, RefNodeFactory.toIRI("http://something")));
        assertThat(nodes.size(), not(is(0)));
    }

    @Test
    public void parsePublishers() throws IOException {

        IRI providedParent = RefNodeFactory.toIRI("someRegistryUUID");
        final List<Quad> nodes = new ArrayList<>();

        InputStream is = publishersInputStream();

        RegistryReaderIDigBio.parsePublishers(providedParent, nodes::add, is);

        assertThat(nodes.size(), is(312));

        Quad node = nodes.get(0);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <51290816-f682-4e38-a06c-03bf5df2442d> ."));

        node = nodes.get(1);
        assertThat(node.toString(), is("<51290816-f682-4e38-a06c-03bf5df2442d> <http://www.w3.org/ns/prov#hadMember> <https://www.morphosource.org/rss/ms.rss> ."));

        node = nodes.get(2);
        assertThat(node.toString(), is("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(3);
        assertThat(node.toString(), startsWith("<https://www.morphosource.org/rss/ms.rss> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(4);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <a9684883-ce9b-4be1-9841-b063fc69e163> ."));

        node = nodes.get(5);
        assertThat(node.toString(), is("<a9684883-ce9b-4be1-9841-b063fc69e163> <http://www.w3.org/ns/prov#hadMember> <http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> ."));

        node = nodes.get(6);
        assertThat(node.toString(), is("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/dc/elements/1.1/format> \"application/rss+xml\" ."));

        node = nodes.get(7);
        assertThat(node.toString(), startsWith("<http://portal.torcherbaria.org/portal/webservices/dwc/rss.xml> <http://purl.org/pav/hasVersion> "));

        node = nodes.get(8);
        assertThat(node.toString(), is("<someRegistryUUID> <http://www.w3.org/ns/prov#hadMember> <089a51fa-5f81-48e7-a1b7-9bc539555f29> ."));

    }

    public InputStream publishersInputStream() {
        return getClass().getResourceAsStream("idigbio-publishers.json");
    }

}