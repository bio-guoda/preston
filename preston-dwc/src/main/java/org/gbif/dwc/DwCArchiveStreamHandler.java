package org.gbif.dwc;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorStateReadOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.gbif.dwc.meta.DwcMetaFiles2;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.file.ClosableIterator;
import org.gbif.utils.file.tabular.TabularDataFileReader;
import org.gbif.utils.file.tabular.TabularFiles;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Set;

public class DwCArchiveStreamHandler implements ContentStreamHandler {

    public static final String META_XML = "meta.xml";
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public DwCArchiveStreamHandler(ContentStreamHandler contentStreamHandler,
                                   Dereferencer<InputStream> inputStreamDereferencer,
                                   OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        if (StringUtils.endsWith(iriString, "/" + META_XML)) {
            try {
                handleAssumedDwCArchive(is, iriString, outputStream, dereferencer, this);
                return true;
            } catch (IOException | SAXException e) {
                throw new ContentStreamException("failed to handle assumed DwC resource [" + iriString + "]", e);
            }
        }
        return false;
    }

    protected static void handleAssumedDwCArchive(InputStream is,
                                                  String iriString,
                                                  OutputStream outputStream,
                                                  Dereferencer<InputStream> dereferencer,
                                                  ProcessorStateReadOnly processorState)
            throws SAXException, IOException, ContentStreamException {
        Archive starRecords = DwcMetaFiles2.fromMetaDescriptor(is);
        ArchiveFile core = starRecords.getCore();

        if (core != null) {
            streamRecords(
                    outputStream,
                    dereferencer,
                    getLocation(iriString, core),
                    "http://rs.tdwg.org/dwc/text/id", processorState
            );

            Set<ArchiveFile> extensions = starRecords.getExtensions();
            for (ArchiveFile extension : extensions) {
                streamRecords(
                        outputStream,
                        dereferencer,
                        getLocation(iriString, extension),
                        "http://rs.tdwg.org/dwc/text/coreid",
                        processorState
                );
            }
        }

    }

    private static void streamRecords(OutputStream outputStream,
                                      Dereferencer<InputStream> dereferencer,
                                      Pair<IRI, ArchiveFile> resourceIRIs,
                                      String idIRI,
                                      ProcessorStateReadOnly processorState) throws ContentStreamException {
        ArchiveFile file = resourceIRIs.getRight();
        try {
            TabularDataFileReader<List<String>> tabularFileReader = createReader(file, resourceIRIs.getLeft(), dereferencer);
            ClosableIterator<Record> iterator = createRecordIterator(file, tabularFileReader);
            while (iterator.hasNext() && processorState.shouldKeepProcessing()) {
                streamAsJson(resourceIRIs, tabularFileReader, iterator.next(), outputStream, idIRI);
            }
        } catch (Throwable ex) {
            rethrowStreamException(ex, resourceIRIs.getLeft().getIRIString());
        }
    }

    private static void rethrowStreamException(Throwable ex, String iriString) throws ContentStreamException {
        if (ex instanceof IllegalStateException && ex.getCause() != null && ex.getCause() instanceof ParseException) {
            ParseException e = (ParseException) ex.getCause();
            iriString = "line:" + iriString + "!/L" + e.getErrorOffset();
        }
        throw new ContentStreamException("failed to handle dwc records from <" + iriString + ">", ex);
    }

    private static void streamAsJson(Pair<IRI, ArchiveFile> resourceIRIs,
                                     TabularDataFileReader<List<String>> tabularFileReader,
                                     Record record,
                                     OutputStream outputStream,
                                     String idIRI) throws IOException {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf("line:" + resourceIRIs.getLeft().getIRIString() + "!/L" + tabularFileReader.getLastRecordLineNumber()));
        objectNode.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf(resourceIRIs.getRight().getRowType().qualifiedName()));

        objectNode.set(idIRI, TextNode.valueOf(record.id()));
        for (Term term : record.terms()) {
            objectNode.set(term.qualifiedName(), TextNode.valueOf(record.value(term)));
        }
        IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), outputStream);
        IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
    }


    private static Pair<IRI, ArchiveFile> getLocation(String iriString, ArchiveFile core) {
        String baseIRI = StringUtils.substring(iriString, 0, StringUtils.length(iriString) - META_XML.length());

        return Pair.of(RefNodeFactory.toIRI(baseIRI + core.getLocation()), core);
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


    private static TabularDataFileReader<List<String>> createReader(ArchiveFile file, IRI resource, Dereferencer<InputStream> dereferencer) throws IOException {
        CharsetDecoder decoder = Charset.forName(file.getEncoding()).newDecoder();
        Reader reader = new InputStreamReader(dereferencer.get(resource), decoder);
        BufferedReader bufferedReader = new BufferedReader(reader);

        return TabularFiles.newTabularFileReader(bufferedReader,
                file.getFieldsTerminatedByChar(),
                file.getLinesTerminatedBy(),
                file.getFieldsEnclosedBy(),
                file.areHeaderLinesIncluded(),
                file.getLinesToSkipBeforeHeader()
        );
    }

    private static ClosableIterator<Record> createRecordIterator(ArchiveFile file,
                                                                 TabularDataFileReader<List<String>> tabularFileReader) {
        return new DwcRecordIterator(
                tabularFileReader,
                file.getId(),
                file.getFields(),
                file.getRowType(),
                false,
                false);
    }


}
