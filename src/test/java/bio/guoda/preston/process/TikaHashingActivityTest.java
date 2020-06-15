package bio.guoda.preston.process;

import bio.guoda.preston.HashGeneratorTLSHTruncatedTest;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static bio.guoda.preston.MimeTypes.MIME_TYPE_JSON;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

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
        }, nodes::add);

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("foo:bar"),
                toIRI("foo:bar"),
                aContentHash));

        Assert.assertThat(nodes.size(), is(0));
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
        }, nodes::add);

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
        }, nodes::add);

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
        }, nodes::add);

        tikaHashing.on(toStatement(
                RefNodeFactory.toIRI("graph:name"),
                aContentHash,
                toIRI("foo:bar"),
                aContentHash));

        Assert.assertThat(nodes.size(), is(10));
        Assert.assertThat(nodes.get(4).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        Assert.assertThat(nodes.get(9).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        Assert.assertThat(nodes.get(9).toString(), is(not(nodes.get(4).toString())));

    }

    public void assertTikaHashGenerations(ArrayList<Quad> nodes) {
        Assert.assertThat(nodes.get(0).toString(), containsString("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Activity>"));
        Assert.assertThat(nodes.get(1).toString(), containsString("<http://www.w3.org/ns/prov#wasInformedBy> <graph:name>"));
        Assert.assertThat(nodes.get(2).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasDerivedFrom> <hash://sha256/blabla> "));
        Assert.assertThat(nodes.get(3).toString(), containsString("<http://www.w3.org/ns/prov#used> <hash://sha256/blabla>"));
        Assert.assertThat(nodes.get(4).toString(), startsWith("<hash://tika-tlsh/532a4237d1782aa7576f40d213f91ce46b1fb886498bebcedc507680db323a9415f> <http://www.w3.org/ns/prov#wasGeneratedBy>"));
        Assert.assertThat(nodes.size(), is(5));
    }


}