
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.ProcessorStateAlwaysContinue;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

public class TracerOfDescendantsTest {

    @Test
    public void comesAfter() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                return getVersion(queryKey);
            }
        };

        ProvenanceTracer tracer = new TracerOfDescendants(hexastore, new ProcessorStateAlwaysContinue());

        List<IRI> iris = new ArrayList<>();

        tracer.trace(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, new StatementListener() {
            @Override
            public void on(Quad statement) {
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
    public void comesAfter2() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                IRI iri;

                if (RefNodeFactory.toIRI(getOlder()).equals(queryKey.getValue())
                        && RefNodeConstants.HAS_PREVIOUS_VERSION.equals(queryKey.getKey())) {
                    iri = RefNodeFactory.toIRI(getNewer());
                } else {
                    iri = getVersion(queryKey);
                }

                return iri;
            }

        };

        ProvenanceTracer tracer = new TracerOfDescendants(hexastore, new ProcessorStateAlwaysContinue());

        List<IRI> iris = new ArrayList<>();

        IRI someCurrent = RefNodeFactory.toIRI(getOlder());

        tracer.trace(someCurrent, new StatementListener() {
            @Override
            public void on(Quad statement) {
                IRI version = VersionUtil.mostRecentVersionForStatement(statement);
                if (version != null) {
                    iris.add(version);
                }
            }
        });

        assertThat(iris.size(), Is.is(1));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI(getNewer())));
    }

    private IRI getVersion(Pair<RDFTerm, RDFTerm> queryKey) {
        IRI iri = null;
        if (RefNodeConstants.BIODIVERSITY_DATASET_GRAPH.equals(queryKey.getKey())
                && RefNodeConstants.HAS_VERSION.equals(queryKey.getValue())) {
            iri = RefNodeFactory.toIRI("some:iri");
        }
        return iri;
    }

    private String getOlder() {
        return "hash://sha256/f5851620a22110d6ebb73809df89c6321e79b4483dd2eb84ea77948505561463";
    }

    private String getNewer() {
        return "hash://sha256/77e30f34ca80fc7e2683e3953d0701a800862b2290d5617e8e5ef8230999e35f";
    }


}