package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.Triple;

public class StatementLogger implements RefStatementListener {

    protected String printStatement(Triple statement) {
        return statement.toString();
    }

    @Override
    public void on(Triple statement) {
        System.out.println(printStatement(statement));
    }



}
