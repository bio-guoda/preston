package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static bio.guoda.preston.MimeTypes.MIME_TYPE_JSON;
import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertTrue;

public class RegistryReaderGBIFTest {

    public static final String GBIFDATASETS_JSON = "gbifdatasets.json";
    public static final String GBIF_OCCURRENCES_JSON = "gbif-occurrences.json";
    public static final String GBIF_INDIVIDUAL_OCCURRENCE_JSON = "gbif-individual-occurrence.json";

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        registryReaderGBIF.on(toStatement(Seeds.GBIF, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        assertThat(new HashSet<>(nodes).size(), is(6));
        assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is("https://api.gbif.org/v1/dataset"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));

        registryReaderGBIF.on(toStatement(toIRI("https://api.gbif.org/v1/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(1));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReaderGBIF.on(toStatement(Seeds.GBIF, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onContinuation() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(GBIFDATASETS_JSON);
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/dataset"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(60974));
        assertThat(nodes.get(18 - 1), hasTriple(toStatement(toIRI("https://api.gbif.org/v1/dataset?offset=2&limit=2"), CREATED_BY, toIRI("https://gbif.org"))));
        assertThat(nodes.get(19 - 1), hasTriple(toStatement(toIRI("https://api.gbif.org/v1/dataset?offset=2&limit=2"), HAS_FORMAT, toLiteral(MIME_TYPE_JSON))));
        Quad secondPage = nodes.get(20 - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<https://api.gbif.org/v1/dataset?offset=2&limit=2>"));
        Quad lastPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(lastPage).toString(), is("<https://api.gbif.org/v1/dataset?offset=40638&limit=2>"));
    }

    @Test
    public void onContinuationSearchOrSuggestion() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("gbif-dataset-search-results.json");
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/dataset/suggest"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(5));
        Quad lastItem = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(lastItem).toString(), is("<https://api.gbif.org/v1/dataset/b7010c1b-8013-4a3c-a43b-4309a91f9629>"));
    }

    @Test
    public void onContinuationSuggestion() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("gbif-suggest.json");
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("http://api.gbif.org/v1/dataset/suggest?q=Amazon&amp;type=OCCURRENCE"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(41));
        Quad lastItem = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(lastItem).toString(), is("<https://api.gbif.org/v1/dataset/663199f1-3528-4289-8069-d27552f62f10>"));
    }

    @Test
    public void onContinuationWithQuery() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(GBIFDATASETS_JSON);
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/dataset/search?q=plant&amp;publishingCountry=AR"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(60974));
        Quad secondPage = nodes.get(20 - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<https://api.gbif.org/v1/dataset/search?q=plant&amp;publishingCountry=AR&offset=2&limit=2>"));
        Quad lastPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(lastPage).toString(), is("<https://api.gbif.org/v1/dataset/search?q=plant&amp;publishingCountry=AR&offset=40638&limit=2>"));
    }


    @Test
    public void onOccurrenceDownload() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                if (key.getIRIString().endsWith("json")) {
                    return getClass().getResourceAsStream("gbif-download-api.json");
                } else {
                    throw new IOException("kaboom!");
                }
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad doiLink = toStatement(toIRI("https://api.gbif.org/v1/occurrence/download/0062961-200221144449610"),
                HAS_VERSION,
                toIRI("hash://json"));

        registryReaderGBIF.on(doiLink);

        assertThat(nodes.size(), is(5));
        Quad secondStatement = nodes.get(1);
        assertThat(secondStatement.toString(), startsWith("<hash://json> <http://www.w3.org/ns/prov#hadMember> <http://api.gbif.org/v1/occurrence/download/request/0062961-200221144449610.zip> "));
    }

    @Test
    public void onOccurrenceDownloadArchive() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad doiLink = toStatement(toIRI("https://api.gbif.org/v1/occurrence/download/request/0062961-200221144449610.zip"),
                HAS_VERSION,
                toIRI("hash://json"));

        registryReaderGBIF.on(doiLink);

        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onSingleDataset() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("gbif-dataset-single.json");
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/dataset"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(6));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<http://plazi.cs.umb.edu/GgServer/dwca/2924FFB8FFC7C76B4B0B503BFFD8D973.zip>"));
    }

    @Test
    public void onSingleOccurrence() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(GBIF_INDIVIDUAL_OCCURRENCE_JSON);
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/occurrence/1234"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(21));
        assertThat(nodes.get(1).toString(),
                startsWith("<https://api.gbif.org/v1/occurrence/1142366485> <http://www.w3.org/ns/prov#hadMember> <http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> <urn:uuid:"));
        assertThat(getVersionSource(nodes.get(4)).toString(),
                is("<http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg>"));
    }

    @Test
    public void isOccurrenceRecordEndpoint() {
        assertTrue(RegistryReaderGBIF.isOccurrenceRecordEndpoint(
                RefNodeFactory.toIRI("https://api.gbif.org/v1/occurrence/1234")));
    }

    @Test
    public void isOccurrenceRecordEndpoint2() {
        assertTrue(RegistryReaderGBIF.isOccurrenceRecordEndpoint(
                RefNodeFactory.toIRI("https://api.gbif.org/v1/occurrence/search?blabla")));
    }

    @Test
    public void onSingleBioCASe() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("gbif-biocase-dataset-single.json");
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.gbif.org/v1/dataset"), HAS_VERSION, createTestNode());

        registryReaderGBIF.on(firstPage);

        assertThat(nodes.size(), is(9));
        Quad secondPage = nodes.get(5);
        assertThat(getVersionSource(secondPage).toString(), is("<http://131.130.131.9/biocase/pywrapper.cgi?dsa=gbif_je>"));
        secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<http://131.130.131.9/biocase/downloads/gbif_je/University%20of%20Jena%2C%20Herbarium%20Haussknecht%20-%20Herbarium%20JE.ABCD_2.06.zip>"));
    }

    @Test
    public void nextPage() {
        List<Quad> nodes = new ArrayList<>();
        RegistryReaderGBIF.emitNextPage(0, 10, TestUtilForProcessor.testEmitter(nodes), "https://bla/?limit=2&offset=8");
        assertThat(nodes.size(), is(3));
        assertThat(nodes.get(1).getSubject().toString(), is("<https://bla/?limit=10&offset=0>"));
    }

    @Test
    public void parseDatasets() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode();

        RegistryReaderGBIF.parseDatasetResultPage(testNode,
                TestUtilForProcessor.testEmitter(refNodes),
                getClass().getResourceAsStream(GBIFDATASETS_JSON),
                toIRI("http://example.org/"));

        assertThat(refNodes.size(), is(60973));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <6555005d-4594-4a3e-be33-c70e587b63d7> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<6555005d-4594-4a3e-be33-c70e587b63d7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#seeAlso> <https://doi.org/10.15468/orx3mk> ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), is("<6555005d-4594-4a3e-be33-c70e587b63d7> <http://www.w3.org/ns/prov#hadMember> <http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> ."));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), is("<http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" ."));

        refNode = refNodes.get(4);
        assertThat(refNode.toString(), startsWith("<http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(5);
        assertThat(refNode.toString(), is("<6555005d-4594-4a3e-be33-c70e587b63d7> <http://www.w3.org/ns/prov#hadMember> <http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> ."));

        refNode = refNodes.get(6);
        assertThat(refNode.toString(), is("<http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> <http://purl.org/dc/elements/1.1/format> \"application/eml\" ."));

        refNode = refNodes.get(7);
        assertThat(refNode.toString(), startsWith("<http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(8);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <d0df772d-78f4-4602-acf2-7d768798f632> ."));

        Quad lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.toString(), is("<http://example.org/?offset=40638&limit=2> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?offset=40638&limit=2> <http://purl.org/pav/hasVersion> "));

    }

    @Test
    public void parseOccurrences() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        RegistryReaderGBIF.parseOccurrenceRecords(
                toIRI("http://example.org/occurrence/123"),
                TestUtilForProcessor.testEmitter(refNodes), getClass().getResourceAsStream(GBIF_OCCURRENCES_JSON),
                toIRI("http://example.org/"));

        assertThat(refNodes.size(), is(55));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), is("<http://example.org/occurrence/123> <http://www.w3.org/ns/prov#hadMember> <https://api.gbif.org/v1/occurrence/1142366485> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), startsWith("<https://api.gbif.org/v1/occurrence/1142366485> <http://purl.org/pav/hasVersion>"));


        Quad lastRefNode = refNodes.get(refNodes.size() - 5);
        assertThat(lastRefNode.toString(), is("<http://example.org/?offset=80&limit=20> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        lastRefNode = refNodes.get(refNodes.size() - 4);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?offset=80&limit=20> <http://purl.org/pav/hasVersion> "));

        lastRefNode = refNodes.get(refNodes.size() - 3);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?offset=100&limit=20> <http://purl.org/pav/createdBy> <https://gbif.org> ."));

        lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.toString(), is("<http://example.org/?offset=100&limit=20> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?offset=100&limit=20> <http://purl.org/pav/hasVersion> "));

    }

    @Test
    public void parseIndividualOccurrence() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        JsonNode occurrence = new ObjectMapper().readTree(
                getClass().getResourceAsStream(GBIF_INDIVIDUAL_OCCURRENCE_JSON));

        RegistryReaderGBIF.parseOccurrenceRecord(
                TestUtilForProcessor.testEmitter(refNodes),
                occurrence
        );

        assertThat(refNodes.size(), is(20));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), is("<https://api.gbif.org/v1/occurrence/1142366485> <http://www.w3.org/ns/prov#hadMember> <http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> <http://xmlns.com/foaf/0.1/depicts> <https://api.gbif.org/v1/occurrence/1142366485> ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), is("<http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> <http://purl.org/dc/elements/1.1/format> \"image/jpeg\" ."));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), startsWith("<http://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> <http://purl.org/pav/hasVersion> _:"));

        refNode = refNodes.get(4);
        assertThat(refNode.toString(), is("<https://api.gbif.org/v1/occurrence/1142366485> <http://www.w3.org/ns/prov#hadMember> <https://fm-digital-assets.fieldmuseum.org/672/421/28936_Menacanthus_campephili_PT_d_IN.jpg> ."));

        Quad lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.toString(), startsWith("<https://fm-digital-assets.fieldmuseum.org/672/422/28936_Menacanthus_campephili_PT_v_IN.jpg> <http://purl.org/pav/hasVersion>"));

    }

    @Test
    public void parseOccurrenceDownload() throws IOException {
        final List<Quad> refNodes = new ArrayList<>();
        IRI testNode = createTestNode();

        RegistryReaderGBIF.parseOccurrenceDownload(
                testNode,
                TestUtilForProcessor.testEmitter(refNodes),
                getClass().getResourceAsStream("gbif-download-api.json"),
                toIRI("http://example.org/"));

        assertThat(refNodes.size(), is(4));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), containsString("/bio/guoda/preston/process/gbifdatasets.json> <http://www.w3.org/ns/prov#hadMember> <http://api.gbif.org/v1/occurrence/download/request/0062961-200221144449610.zip> "));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), containsString("<http://api.gbif.org/v1/occurrence/download/request/0062961-200221144449610.zip> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), containsString("/bio/guoda/preston/process/gbifdatasets.json> <http://www.w3.org/ns/prov#hadMember> <https://doi.org/10.15468/dl.4n9w6m> "));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), startsWith("<http://api.gbif.org/v1/occurrence/download/request/0062961-200221144449610.zip> <http://www.w3.org/1999/02/22-rdf-syntax-ns#seeAlso> <https://doi.org/10.15468/dl.4n9w6m> "));


    }

    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("gbifdatasets.json").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private IRI createOccurrencesTestNode() {
        try {
            return toIRI(getClass().getResource(GBIF_OCCURRENCES_JSON).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private IRI createIndividualOccurrenceTestNode() {
        try {
            return toIRI(getClass().getResource("gbif-individual-occurrence.json").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}