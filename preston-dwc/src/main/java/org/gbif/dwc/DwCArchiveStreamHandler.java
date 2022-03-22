package org.gbif.dwc;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.gbif.dwc.meta.DwcMetaFiles;
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
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DwCArchiveStreamHandler implements ContentStreamHandler {

    public static final String META_XML = "meta.xml";
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;

    public DwCArchiveStreamHandler(ContentStreamHandler contentStreamHandler, Dereferencer<InputStream> inputStreamDereferencer) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        String iriString = version.getIRIString();
        try {
            if (StringUtils.endsWith(iriString, "/" + META_XML)) {
                Archive starRecords = DwcMetaFiles.fromMetaDescriptor(is);
                ArchiveFile core = starRecords.getCore();

                List<Pair<IRI, ArchiveFile>> dwcaResourceIRIs = new ArrayList<>();


                dwcaResourceIRIs.add(getLocation(iriString, core));
                Set<ArchiveFile> extensions = starRecords.getExtensions();
                for (ArchiveFile extension : extensions) {
                    dwcaResourceIRIs.add(getLocation(iriString, extension));
                }


                for (Pair<IRI, ArchiveFile> resourceIRIs : dwcaResourceIRIs) {
                    ArchiveFile file = resourceIRIs.getRight();
                    try {
                        TabularDataFileReader<List<String>> tabularFileReader = createReader(file, resourceIRIs.getLeft());
                        ClosableIterator<Record> iterator = createRecordIterator(file, tabularFileReader);
                        while (iterator.hasNext()) {
                            streamAsJson(resourceIRIs, tabularFileReader, iterator.next());
                        }
                    } catch (IOException ex) {
                        throw new ContentStreamException("failed to read or stream dwc records", ex);
                    }
                }

                return true;
            }
        } catch (IOException | SAXException e) {
            throw new ContentStreamException("failed to parse [" + iriString + "]", e);
        }
        return false;
    }

    private void streamAsJson(Pair<IRI, ArchiveFile> resourceIRIs, TabularDataFileReader<List<String>> tabularFileReader, Record next) {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf("line:" + resourceIRIs.getLeft().getIRIString() + "!/L" + tabularFileReader.getLastRecordLineNumber()));
        objectNode.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf(resourceIRIs.getRight().getRowType().qualifiedName()));
        for (Term term : next.terms()) {
            objectNode.set(term.qualifiedName(), TextNode.valueOf(next.value(term)));
        }
        System.out.println(objectNode.toString());
    }


    private Pair<IRI, ArchiveFile> getLocation(String iriString, ArchiveFile core) {
        String baseIRI = StringUtils.substring(iriString, 0, StringUtils.length(iriString) - META_XML.length());

        return Pair.of(RefNodeFactory.toIRI(baseIRI + core.getLocation()), core);
    }

    @Override
    public boolean shouldKeepReading() {
        return contentStreamHandler.shouldKeepReading();
    }


    private TabularDataFileReader<List<String>> createReader(ArchiveFile file, IRI resource) throws IOException {
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

    private static ClosableIterator<Record> createRecordIterator(ArchiveFile file, TabularDataFileReader<List<String>> tabularFileReader) {
        return new DwcRecordIterator(
                tabularFileReader,
                file.getId(),
                file.getFields(),
                file.getRowType(),
                false,
                false);
    }



}
