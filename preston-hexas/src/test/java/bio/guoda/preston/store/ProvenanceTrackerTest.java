
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class ProvenanceTrackerTest {

    @Test
    public void comesAfter() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                return getVersion(queryKey);
            }
        };

        ProvenanceTracker tracker = new ProvenanceTrackerImpl(hexastore);

        List<IRI> iris = new ArrayList<>();

        tracker.findDescendants(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, new VersionListener() {
            @Override
            public void on(Quad statement) throws IOException {
                IRI version = VersionUtil.mostRecentVersionForStatement(statement);
                if (version != null) {
                    iris.add(version);
                }
            }
        });

        assertThat(iris.size(), Is.is(1));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI("some:iri")));
    }

    @Test
    public void cameBefore() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                IRI iri = null;

                if (RefNodeFactory.toIRI("some:older/iri").equals(queryKey.getValue())
                    && RefNodeConstants.HAS_PREVIOUS_VERSION.equals(queryKey.getKey())) {
                    iri = RefNodeFactory.toIRI("some:newer/iri");
                } else {
                    iri = getVersion(queryKey);
                }

                return iri;
            }

        };

        ProvenanceTracker tracker = new ProvenanceTrackerImpl(hexastore);

        List<IRI> iris = new ArrayList<>();

        IRI someCurrent = RefNodeFactory.toIRI("some:older/iri");

        tracker.findDescendants(someCurrent, new VersionListener() {
            @Override
            public void on(Quad statement) throws IOException {
                IRI version = VersionUtil.mostRecentVersionForStatement(statement);
                if (version != null) {
                    iris.add(version);
                }
            }
        });

        assertThat(iris.size(), Is.is(1));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI("some:newer/iri")));
    }

    IRI getVersion(Pair<RDFTerm, RDFTerm> queryKey) {
        IRI iri = null;
        if (RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(queryKey.getKey())
                && RefNodeConstants.HAS_VERSION.equals(queryKey.getValue())) {
            iri = RefNodeFactory.toIRI("some:iri");
        }
        return iri;
    }

}