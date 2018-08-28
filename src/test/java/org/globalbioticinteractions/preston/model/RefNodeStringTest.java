package org.globalbioticinteractions.preston.model;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RefNodeStringTest {

    @Test
    public void equivalent() {
        RefNode one = new RefNodeString(RefNodeType.URI, "https://example.com");
        RefNode two = new RefNodeString(RefNodeType.URI, "https://example.com");
        assertTrue(one.equivalentTo(two));
    }

    @Test
    public void differentValue() {
        RefNode one = new RefNodeString(RefNodeType.URI, "https://example.com/2");
        RefNode two = new RefNodeString(RefNodeType.URI, "https://example.com");
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentType() {
        RefNode one = new RefNodeString(RefNodeType.URI, "https://example.com");
        RefNode two = new RefNodeString(RefNodeType.DWCA, "https://example.com");
        assertFalse(one.equivalentTo(two));
    }

    @Test
    public void differentClass() {
        RefNode one = new RefNodeString(RefNodeType.URI, "https://example.com");
        RefNode two = new RefNodeURI(RefNodeType.URI, URI.create("https://example.com"));
        assertFalse(one.equivalentTo(two));
    }

}