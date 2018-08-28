package org.globalbioticinteractions.preston.model;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class RefNodeURITest {

    @Test
    public void equivalentTo() {
        RefNode one = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        RefNode two = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        assertTrue(one.equivalentTo(two));
    }

    @Test
    public void differentTypes() {
        RefNode one = new RefNodeURI(RefNodeType.EML, URI.create("https://example.com"));
        RefNode two = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentSubclass() {
        RefNode one = new RefNodeString(RefNodeType.URI, "https://example.com");
        RefNode two = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentValue() {
        RefNodeURI one = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        RefNodeURI two = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com/2"));
        assertTrue(!one.equivalentTo(two));
    }

}