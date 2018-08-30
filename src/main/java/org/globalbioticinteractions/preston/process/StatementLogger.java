package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public class StatementLogger implements RefStatementListener {

    protected String printStatement(RefStatement statement) {
        return statement.getSubject().getLabel() + "\t" + statement.getPredicate().getLabel() + "\t" + statement.getObject().getLabel();
    }

    @Override
    public void on(RefStatement statement) {
        System.out.println(printStatement(statement));
    }



}
