package org.globalbioticinteractions.preston.model;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RefStatementTest {
    private RefNode source = new RefNodeURI(URI.create("https://example.org"));
    private RefNode relation = new RefNodeURI(URI.create("https://example.org/partOf"));
    private RefNode relation2 = new RefNodeURI(URI.create("https://example.org/sortOf"));
    private RefNode target = new RefNodeURI(URI.create("https://example.org/a"));
    private RefNode target2 = new RefNodeURI(URI.create("https://example.org/b"));

    @Test
    public void equivalent() {
        RefStatement link = new RefStatement(source, relation, target);
        RefStatement link2 = new RefStatement(source, relation, target);
        assertTrue(link.equivalentTo(link2));
    }

    @Test
    public void notEquivalent() {
        RefStatement link = new RefStatement(source, relation, target);
        RefStatement link2 = new RefStatement(source, relation, target2);
        assertFalse(link.equivalentTo(link2));
    }

    @Test
    public void notEquivalent2() {
        RefStatement link = new RefStatement(source, relation, target);
        RefStatement link2 = new RefStatement(source, relation2, target);
        assertFalse(link.equivalentTo(link2));
    }

}