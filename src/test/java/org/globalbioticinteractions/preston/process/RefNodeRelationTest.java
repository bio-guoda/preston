package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RefNodeRelationTest {
    private RefNodeString source = new RefNodeString(null, RefNodeType.URI, "https://example.org");
    private RefNodeString relation = new RefNodeString(null, RefNodeType.URI, "https://example.org/partOf");
    private RefNodeString relation2 = new RefNodeString(null, RefNodeType.URI, "https://example.org/sortOf");
    private RefNodeString target = new RefNodeString(null, RefNodeType.URI, "https://example.org/a");
    private RefNodeString target2 = new RefNodeString(null, RefNodeType.URI, "https://example.org/b");

    @Test
    public void equivalent() {
        RefNodeRelation link = new RefNodeRelation(source, relation, target);
        RefNodeRelation link2 = new RefNodeRelation(source, relation, target);
        assertTrue(link.equivalentTo(link2));
    }

    @Test
    public void notEquivalent() {
        RefNodeRelation link = new RefNodeRelation(source, relation, target);
        RefNodeRelation link2 = new RefNodeRelation(source, relation, target2);
        assertFalse(link.equivalentTo(link2));
    }

    @Test
    public void notEquivalent2() {
        RefNodeRelation link = new RefNodeRelation(source, relation, target);
        RefNodeRelation link2 = new RefNodeRelation(source, relation2, target);
        assertFalse(link.equivalentTo(link2));
    }

}