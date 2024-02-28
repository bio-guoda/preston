package bio.guoda.preston.excel;

import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.commons.rdf.api.IRI;
import org.apache.poi.openxml4j.exceptions.InvalidOperationException;
import org.apache.poi.openxml4j.exceptions.NotOfficeXmlFileException;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.OutputStream;

public class XLSXHandler {


    public static void asJsonStream(OutputStream out, IRI resourceIRI, KeyValueStoreReadOnly contentStore, Integer skipLines, Boolean headerless) throws IOException {
        try (Workbook workbook = StreamingReader
                .builder()
                .open(contentStore.get(resourceIRI))) {
            XLSHandler.asJsonStream(
                    out,
                    resourceIRI,
                    workbook,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    skipLines,
                    headerless);
        } catch (NotOfficeXmlFileException | InvalidOperationException ex) {
            // ignore runtime exception to implement opportunistic handling
        }
    }
}