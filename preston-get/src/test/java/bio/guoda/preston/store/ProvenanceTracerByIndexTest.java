
package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProvenanceTracerByIndexTest {

    @Test
    public void comesAfter() throws IOException {

        HexaStoreReadOnly hexastore = new HexaStoreReadOnly() {
            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                return getVersion(queryKey);
            }
        };

        ProvenanceTracer tracer = new ProvenanceTracerByIndex(hexastore, new ProvenanceTracerImpl(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                InputStream is = null;
                if (uri.getIRIString().equals("some:iri")) {
                    is = IOUtils.toInputStream("<foo:bar> <foo:bar> <foo:bar> .", StandardCharsets.UTF_8);
                }
                return is;
            }
        }, new ProcessorStateAlwaysContinue()));

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

        ProvenanceTracer tracer = new ProvenanceTracerByIndex(hexastore, new ProvenanceTracerImpl(new KeyValueStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                InputStream is = null;
                if (uri.getIRIString().equals("some:iri")) {
                    is = IOUtils.toInputStream("<foo:bar> <foo:bar> <foo:bar> .", StandardCharsets.UTF_8);
                } else if (uri.getIRIString().equals(getOlder())) {
                    is = IOUtils.toInputStream("<foo:bar> <foo:bar> <foo:bar> .", StandardCharsets.UTF_8);
                } else if (uri.getIRIString().equals(getNewer())) {
                    is = IOUtils.toInputStream(RefNodeFactory.toStatement(RefNodeFactory.toIRI(getOlder()), USED_BY, RefNodeFactory.toIRI("foo:bar")) + "\n", StandardCharsets.UTF_8);
                }
                return is;
            }
        }, new ProcessorStateAlwaysContinue()));

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

        assertThat(iris.size(), Is.is(2));
        assertThat(iris.get(0), Is.is(RefNodeFactory.toIRI(getNewer())));
        assertThat(iris.get(1), Is.is(RefNodeFactory.toIRI(getOlder())));
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