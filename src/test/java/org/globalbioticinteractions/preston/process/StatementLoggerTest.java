package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class StatementLoggerTest {

    @Test
    public void relation() {
        IRI source = RefNodeFactory.toIRI("source");
        IRI relation = RefNodeFactory.toIRI("relation");
        RDFTerm target = RefNodeFactory.toLiteral("target");

        String str = new StatementLogger().printStatement(RefNodeFactory.toStatement(source, relation, target));

        assertThat(str, is("<source> <relation> \"target\" ."));
    }


}