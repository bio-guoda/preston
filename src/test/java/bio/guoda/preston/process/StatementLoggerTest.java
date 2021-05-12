package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class StatementLoggerTest {

    @Test
    public void print() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new StatementLogger(new PrintStream(out)) {
            @Override
            public void on(Quad statement) {
                print("print me");
            }
        }.on(RefNodeFactory.toStatement(toIRI(""), toIRI(""), toLiteral("")));

        assertThat(StringUtils.toEncodedString(out.toByteArray(), StandardCharsets.UTF_8),
                is("print me"));
    }

    @Test
    public void printWithError() {
        PrintStream printStream = new PrintStream(new ByteArrayOutputStream());
        printStream.close();

        AtomicBoolean boom = new AtomicBoolean(false);
        new StatementLogger(printStream, () -> boom.set(true)) {
            @Override
            public void on(Quad statement) {
                print("print me");
            }
        }.on(RefNodeFactory.toStatement(toIRI(""), toIRI(""), toLiteral("")));

        assertTrue(boom.get());
    }

}