package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
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
        String innerURIString = HashKeyUtil.extractInnerURI("blablabla");
        assertThat(innerURIString, Is.is(nullValue()));
    }


}