package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.junit.Test;

import static org.junit.Assert.*;

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
    public void validComboHashURI() {
        String comboHashURI = "cut:hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca!/b217065-217087";
        boolean matches = HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI));
        assertTrue(matches);
    }

    @Test
    public void validComboHashURIPlainHashURI() {
        String comboHashURI = "hash://sha256/e0c131ebf6ad2dce71ab9a10aa116dcedb219ae4539f9e5bf0e57b84f51f22ca";
        boolean matches = HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI));
        assertTrue(matches);
    }

    @Test
    public void invalidComboHashURI() {
        String comboHashURI = "foo:bar:123";
        assertFalse(HashKeyUtil.isLikelyCompositeHashURI(RefNodeFactory.toIRI(comboHashURI)));
    }

}