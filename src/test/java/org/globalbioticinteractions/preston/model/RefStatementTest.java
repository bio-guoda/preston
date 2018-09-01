package org.globalbioticinteractions.preston.model;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RefStatementTest {
    private RefNodeString source = new RefNodeString("https://example.org");
    private RefNodeString relation = new RefNodeString("https://example.org/partOf");
    private RefNodeString relation2 = new RefNodeString("https://example.org/sortOf");
    private RefNodeString target = new RefNodeString("https://example.org/a");
    private RefNodeString target2 = new RefNodeString("https://example.org/b");

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