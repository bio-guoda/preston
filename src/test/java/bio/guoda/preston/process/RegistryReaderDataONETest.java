package bio.guoda.preston.process;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class RegistryReaderDataONETest {

    public static final String FIRST_PAGE = "http://cn.dataone.org/cn/v2/query/solr/?q=formatId:eml*+AND+-obsoletedBy:*&fl=identifier,dataUrl&wt=json&start=0&rows=100";
    public static final String DATAONE_FIRST_JSON = "dataone-first.json";

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderDataONE reader = new RegistryReaderDataONE(TestUtil.getTestBlobStore(), nodes::add);
        reader.on(toStatement(Seeds.DATA_ONE, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        Assert.assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is(FIRST_PAGE));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderDataONE reader = new RegistryReaderDataONE(TestUtil.getTestBlobStore(), nodes::add);

        reader.on(toStatement(toIRI(FIRST_PAGE),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderDataONE reader = new RegistryReaderDataONE(TestUtil.getTestBlobStore(), nodes::add);
        RDFTerm bla = toLiteral("bla");
        reader.on(toStatement(Seeds.DATA_ONE, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onContinuation() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(DATAONE_FIRST_JSON);
            }
        };
        RegistryReaderDataONE reader = new RegistryReaderDataONE(blobStore, nodes::add);


        Quad firstPage = toStatement(toIRI(FIRST_PAGE), HAS_VERSION, createTestNode());

        reader.on(firstPage);

        Assert.assertThat(nodes.size(), is(43));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<http://cn.dataone.org/cn/v2/query/solr/?q=formatId:eml*+AND+-obsoletedBy:*&fl=identifier,dataUrl&wt=json&start=1000&rows=1000>"));
    }


    @Test
    public void onContinuationWithQuery() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(DATAONE_FIRST_JSON);
            }
        };
        RegistryReaderDataONE reader = new RegistryReaderDataONE(blobStore, nodes::add);


        Quad firstPage = toStatement(toIRI(FIRST_PAGE), HAS_VERSION, createTestNode());

        reader.on(firstPage);

        Assert.assertThat(nodes.size(), is(43));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(secondPage.toString(), startsWith("<http://cn.dataone.org/cn/v2/query/solr/?q=formatId:eml*+AND+-obsoletedBy:*&fl=identifier,dataUrl&wt=json&start=1000&rows=1000> <http://purl.org/pav/hasVersion> _:"));
    }

    @Test
    public void onSolrResults() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(DATAONE_FIRST_JSON);
            }
        };
        RegistryReaderDataONE reader = new RegistryReaderDataONE(blobStore, nodes::add);


        Quad firstPage = toStatement(toIRI(FIRST_PAGE), HAS_VERSION, createTestNode());

        reader.on(firstPage);

        Assert.assertThat(nodes.size(), is(43));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<http://cn.dataone.org/cn/v2/query/solr/?q=formatId:eml*+AND+-obsoletedBy:*&fl=identifier,dataUrl&wt=json&start=1000&rows=1000>"));
    }

    @Test
    public void onObjectLocationList() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("dataone-object-location-list.xml");
            }
        };
        RegistryReaderDataONE reader = new RegistryReaderDataONE(blobStore, nodes::add);

        Quad firstPage = toStatement(toIRI("https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FKM_CUDM.20160202"), HAS_VERSION, createTestNode());

        reader.on(firstPage);

        Assert.assertThat(nodes.size(), is(20));
        Quad identifierMemberStatement = nodes.get(1);
        assertThat(identifierMemberStatement.toString(), is("<aekos.org.au/collection/nsw.gov.au/nsw_atlas/vis_flora_module/NOMBIN.20150515> <http://www.w3.org/ns/prov#hadMember> <https://dataone.tern.org.au/mn/v2/object/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FNOMBIN.20150515> ."));
        Quad locationType = nodes.get(2);
        assertThat(locationType.toString(), is("<https://dataone.tern.org.au/mn/v2/object/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FNOMBIN.20150515> <http://purl.org/dc/elements/1.1/format> \"application/eml\" ."));
        Quad locationVersion = nodes.get(3);
        assertThat(locationVersion.toString(), startsWith("<https://dataone.tern.org.au/mn/v2/object/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FNOMBIN.20150515> <http://purl.org/pav/hasVersion> _:"));
        Quad nodeIdentifierMemberStatement = nodes.get(4);
        assertThat(nodeIdentifierMemberStatement.toString(), is("<urn:node:CN> <http://www.w3.org/ns/prov#hadMember> <aekos.org.au/collection/nsw.gov.au/nsw_atlas/vis_flora_module/NOMBIN.20150515> ."));
    }

    @Test
    public void nextPage() {
        List<Quad> nodes = new ArrayList<>();
        RegistryReaderDataONE.emitNextPage(0, 10, nodes::add, "https://bla/?rows=2&start=8");
        assertThat(nodes.size(), is(3));
        assertThat(nodes.get(1).getSubject().toString(), is("<https://bla/?rows=10&start=0>"));
    }

    @Test
    public void parseDatasets() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode();

        RegistryReaderDataONE.parse(testNode, refNodes::add, getClass().getResourceAsStream(DATAONE_FIRST_JSON), toIRI("http://example.org/"));

        assertThat(refNodes.size(), is(43));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <aekos.org.au/collection/nsw.gov.au/nsw_atlas/vis_flora_module/KM_CUDM.20160202> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<aekos.org.au/collection/nsw.gov.au/nsw_atlas/vis_flora_module/KM_CUDM.20160202> <http://www.w3.org/ns/prov#hadMember> <https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FKM_CUDM.20160202> ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), is("<https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FKM_CUDM.20160202> <http://purl.org/dc/elements/1.1/format> \"text/xml\" ."));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), startsWith("<https://cn.dataone.org/cn/v2/resolve/aekos.org.au%2Fcollection%2Fnsw.gov.au%2Fnsw_atlas%2Fvis_flora_module%2FKM_CUDM.20160202> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(4);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <aekos.org.au/collection/sa.gov.au/DEWNR_ROADSIDEVEG/45.20160201> ."));


        Quad lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.toString(), is("<http://example.org/?start=1000&rows=1000> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?start=1000&rows=1000> <http://purl.org/pav/hasVersion> _:"));

        lastRefNode = refNodes.get(refNodes.size() - 3);
        assertThat(lastRefNode.toString(), is("<http://example.org/?start=1000&rows=1000> <http://purl.org/pav/createdBy> <https://dataone.org> ."));

    }

    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource(DATAONE_FIRST_JSON).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}