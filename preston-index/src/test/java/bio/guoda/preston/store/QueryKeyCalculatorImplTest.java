package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class QueryKeyCalculatorImplTest {

    @Test
    public void calculateKey() {
        IRI publisher = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/publishers");
        IRI version = RefNodeFactory.toIRI("http://purl.org/pav/hasVersion");

        assertThat(HexaStoreImpl.calculateHashFor(publisher, HashType.sha256).getIRIString(),
                is("hash://sha256/3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef"));

        assertThat(HexaStoreImpl.calculateHashFor(version, HashType.sha256).getIRIString(),
                is("hash://sha256/0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806"));

        IRI iri = new QueryKeyCalculatorImpl(HashType.sha256).calculateKeyFor(Pair.of(publisher, version));
        assertThat(iri.getIRIString(),
                is("hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e"));
    }

    @Test
    public void calculateRootArchiveQueryKey() {
        IRI iri = new QueryKeyCalculatorImpl(HashType.sha256).calculateKeyFor(
                Pair.of(RefNodeFactory.toIRI(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_UUID_STRING),
                        RefNodeConstants.HAS_VERSION)
        );

        assertThat(iri.getIRIString(),
                is("hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"));
        assertThat(iri.getIRIString(),
                is(RefNodeConstants.PROVENANCE_ROOT_QUERY_HASH_SHA256));
    }

    @Test
    public void calculateRootURN_UUID_ArchiveQueryKey() {
        IRI iri = new QueryKeyCalculatorImpl(HashType.sha256)
                .calculateKeyFor(Pair.of(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_URN_UUID, RefNodeConstants.HAS_VERSION));
        assertThat(iri.getIRIString(),
                is("hash://sha256/7fd737aed8f0d9acefb35db505cd77e81157dbf23c51203db53b00dea68af467"));
    }



}