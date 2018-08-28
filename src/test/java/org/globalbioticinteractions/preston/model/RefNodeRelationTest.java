package org.globalbioticinteractions.preston.model;

import org.hamcrest.core.Is;
import org.junit.Test;

import static com.sun.org.apache.xerces.internal.util.PropertyState.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RefNodeRelationTest {
    private RefNodeString source = new RefNodeString(RefNodeType.URI, "https://example.org");
    private RefNodeString relation = new RefNodeString(RefNodeType.URI, "https://example.org/partOf");
    private RefNodeString relation2 = new RefNodeString(RefNodeType.URI, "https://example.org/sortOf");
    private RefNodeString target = new RefNodeString(RefNodeType.URI, "https://example.org/a");
    private RefNodeString target2 = new RefNodeString(RefNodeType.URI, "https://example.org/b");

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

    @Test
    public void some() {
        RefNodeRelation link = new RefNodeRelation(source, relation, target);
        assertThat(link.getId(), Is.is("59c43e4e2105bce3762c5b7ba910133aae92c938b13cb09c027d02306de66b4f"));
    }

}