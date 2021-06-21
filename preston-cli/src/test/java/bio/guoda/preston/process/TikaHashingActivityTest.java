package bio.guoda.preston.process;

import bio.guoda.preston.HashGeneratorTLSHTruncatedTest;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Ignore
public class TikaHashingActivityTest {

    @Test
    public void contentWithLowEntropy() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        ArrayList<Quad> nodes = new ArrayList<>();
        TikaHashingActivity tikaHashing = new TikaHashingActivity(new BlobStoreReadOnly(){

            @Override
            public InputStream get(IRI key) throws IOException {
                if (!aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return IOUtils.toInputStream("bla", StandardCharsets.UTF_8);

            }
        }, TestUtilForProcessor.testListener(nodes));

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                toIRI("foo:bar"),
                aContentHash));

        assertThat(nodes.size(), is(0));
    }


    @Test
    public void onHashAsObject() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        ArrayList<Quad> nodes = new ArrayList<>();
        TikaHashingActivity tikaHashing = new TikaHashingActivity(new BlobStoreReadOnly(){

            @Override
            public InputStream get(IRI key) throws IOException {
                if (!aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream(HashGeneratorTLSHTruncatedTest.DWCA);

            }
        }, TestUtilForProcessor.testListener(nodes));

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("graph:name"),
                RefNodeFactory.toIRI("foo:bar"),
                toIRI("foo:bar"),
                aContentHash));

        assertTikaHashGenerations(nodes);
    }

    @Test
    public void onHashAsSubject() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        ArrayList<Quad> nodes = new ArrayList<>();
        TikaHashingActivity tikaHashing = new TikaHashingActivity(new BlobStoreReadOnly(){

            @Override
            public InputStream get(IRI key) throws IOException {
                if (!aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream(HashGeneratorTLSHTruncatedTest.DWCA);

            }
        }, TestUtilForProcessor.testListener(nodes));

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("graph:name"),
                aContentHash,
                toIRI("foo:bar"),
                toIRI("foo:bar")));

        assertTikaHashGenerations(nodes);
    }


    @Test
    public void onHashAsSubjectAndObject() {
        IRI aContentHash = toIRI("hash://sha256/blabla");

        ArrayList<Quad> nodes = new ArrayList<>();
        TikaHashingActivity tikaHashing = new TikaHashingActivity(new BlobStoreReadOnly(){

            @Override
            public InputStream get(IRI key) throws IOException {
                if (!aContentHash.equals(key)) {
                    throw new IOException("kaboom!");
                }
                return getClass().getResourceAsStream(HashGeneratorTLSHTruncatedTest.DWCA);

            }
        }, TestUtilForProcessor.testListener(nodes));

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("graph:name"),
                aContentHash,
                toIRI("foo:bar"),
                aContentHash));

        assertThat(nodes.size(), is(8));
        assertThat(nodes.get(3).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        assertThat(nodes.get(7).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        assertThat(nodes.get(7).toString(), is(not(nodes.get(3).toString())));

    }

    public void assertTikaHashGenerations(ArrayList<Quad> nodes) {
        assertThat(nodes.get(0).toString(), containsString("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Activity>"));
        assertThat(nodes.get(1).toString(), containsString("<http://www.w3.org/ns/prov#wasInformedBy> <graph:name>"));
        assertThat(nodes.get(2).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/blabla> "));
        assertThat(nodes.get(3).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        assertThat(nodes.size(), is(4));
    }


}