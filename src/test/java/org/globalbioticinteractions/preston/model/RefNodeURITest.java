package org.globalbioticinteractions.preston.model;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class RefNodeURITest {

    @Test
    public void equivalentTo() {
        RefNode one = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com"));
        RefNode two = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com"));
        assertTrue(one.equivalentTo(two));
    }

    @Test
    public void differentTypes() {
        RefNode one = new RefNodeURI(null, RefNodeType.EML, URI.create("https://example.com"));
        RefNode two = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com"));
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentSubclass() {
        RefNode one = new RefNodeString(null, RefNodeType.URI, "https://example.com");
        RefNode two = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com"));
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentValue() {
        RefNodeURI one = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com"));
        RefNodeURI two = new RefNodeURI(null, RefNodeType.URI, URI.create("https://example.com/2"));
        assertTrue(!one.equivalentTo(two));
    }

}