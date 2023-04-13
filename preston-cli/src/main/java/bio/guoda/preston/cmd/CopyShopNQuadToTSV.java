package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.StatementLoggerTSV;
import bio.guoda.preston.process.CmdUtil;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.ProcessorStateReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.RDF;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Consumer;

public class CopyShopNQuadToTSV implements CopyShop {

    private final ProcessorStateReadOnly context;

    public CopyShopNQuadToTSV(ProcessorStateReadOnly context) {
        this.context = context;
    }

    @Override
    public void copy(InputStream is, OutputStream os) throws IOException {
        RDFParser parser = Rio.createParser(RDFFormat.NQUADS);
        parser.setRDFHandler(new AbstractRDFHandler() {
            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                String subject = statement.getSubject().stringValue();
                String predicate = statement.getPredicate().stringValue();
                String object = statement.getObject().stringValue();
                Resource context = statement.getContext();
                String graphName = context == null ? "" : context.stringValue();

                try {
                    IOUtils.write(subject + "\t" + predicate + "\t" + object + "\t" + graphName + "\n", os, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RDFHandlerException("failed to generate tsv", e);
                }
            }
        });

        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while (getContext().shouldKeepProcessing() && (line = reader.readLine()) != null) {
            parser.parse(IOUtils.toInputStream(line, StandardCharsets.UTF_8));
        }
    }

    public ProcessorStateReadOnly getContext() {
        return context;
    }
}
