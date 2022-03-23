package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class HashKeyUtilTest {

    @Test
    public void validHashURI() {
        String hashURIString = "hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca";
        assertTrue(HashKeyUtil
                .isValidHashKey(RefNodeFactory.toIRI(hashURIString)));
    }

    @Test
    public void invalidHashURI() {
        String hashURIString = "hash://sha256/e0c131ebf6ad2dce";
        assertFalse(HashKeyUtil
                .isValidHashKey(RefNodeFactory.toIRI(hashURIString)));
    }

    @Test
    public void invalidHashURI2() {
        String hashURIString = "foo:bar:e0c131ebf6ad2dce";
        assertFalse(HashKeyUtil
                .isValidHashKey(RefNodeFactory.toIRI(hashURIString)));
    }

    @Test
    public void validCompositeHashURI() {
        String comboHashURI = "cut:hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca!/b217065-217087";
        assertTrue(HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void validCompositeHashURIMD5() {
        String comboHashURI = "cut:hash://md5/a3d9b3fca6e594d6d5ff64308a76ddb3!/b217065-217087";
        assertTrue(HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void validCompositeHashURIPlainHashURI() {
        String comboHashURI = "hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca";
        assertTrue(HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void invalidCompositeHashURI() {
        String comboHashURI = "foo:bar:123";
        assertFalse(HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void likelyCompositeURI() {
        String comboHashURI = "foo:bar:123";
        assertTrue(HashKeyUtil.isLikelyCompositeURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void likelyCompositeURIWithPath() {
        String comboHashURI = "foo:bar:123!/donald";
        assertTrue(HashKeyUtil.isLikelyCompositeURI(RefNodeFactory.toIRI(comboHashURI)));
    }


    @Test
    public void unlikelyCompositeURI() {
        String comboHashURI = "foo";
        assertFalse(HashKeyUtil.isLikelyCompositeURI(RefNodeFactory.toIRI(comboHashURI)));
    }

    @Test
    public void parseCompositeURI() {
        String uriString = "gz:file:foo.txt.gz!/foo.txt";
        assertThat(HashKeyUtil.extractInnerURI(uriString), Is.is("file:foo.txt.gz"));
    }

    @Test
    public void parseCompositeURI2() {
        String uriString = "tar:gz:file:foo.txt.tar.gz!/foo.txt";
        assertThat(
                HashKeyUtil.extractInnerURI(uriString),
                Is.is("file:foo.txt.tar.gz")
        );
    }

    @Test
    public void parseCompositeURI3() {
        assertThat(
                HashKeyUtil.extractInnerURI("gz:https://example.org/foo.txt.gz!/foo.txt"),
                Is.is("https://example.org/foo.txt.gz")
        );
    }

    @Test
    public void parseNonCompositeURI3() {
        String innerURIString = HashKeyUtil.extractInnerURI("foo:bar");
        assertThat(innerURIString, Is.is("foo:bar"));
    }

    @Test
    public void extractContentHashSHA256() {
        IRI contentHash = HashKeyUtil.extractContentHash(toIRI("blub:hash://sha256/babababababababababababababababababababababababababababababababa!/blah"));
        assertThat(contentHash.getIRIString(), is("hash://sha256/babababababababababababababababababababababababababababababababa"));
    }

    @Test
    public void extractContentHashMD5() {
        IRI contentHash = HashKeyUtil.extractContentHash(toIRI("blub:hash://md5/b1946ac92492d2347c6235b4d2611184!/blah"));
        assertThat(contentHash.getIRIString(), is("hash://md5/b1946ac92492d2347c6235b4d2611184"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractNonContentHash() {
        HashKeyUtil.extractContentHash(toIRI("urn:uuid:619e692b-e75d-487b-b80e-0f86958be405"));
    }

}