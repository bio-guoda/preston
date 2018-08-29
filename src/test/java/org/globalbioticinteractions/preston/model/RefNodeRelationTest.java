package org.globalbioticinteractions.preston.model;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RefNodeRelationTest {
    private RefNodeString source = new RefNodeString("https://example.org");
    private RefNodeString relation = new RefNodeString("https://example.org/partOf");
    private RefNodeString relation2 = new RefNodeString("https://example.org/sortOf");
    private RefNodeString target = new RefNodeString("https://example.org/a");
    private RefNodeString target2 = new RefNodeString("https://example.org/b");

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
        assertThat(link.getId().toString(), Is.is("hash://sha256/4cb8deba3876d36c1b217a472c045d2d9a81ee59424f7fc37102a9bb13d543c7"));
    }

}