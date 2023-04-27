package bio.guoda.preston.paradox;

import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.googlecode.paradox.ConnectionInfo;
import com.googlecode.paradox.data.ParadoxData;
import com.googlecode.paradox.data.TableData;
import com.googlecode.paradox.data.filefilters.TableFilter;
import com.googlecode.paradox.metadata.Field;
import com.googlecode.paradox.metadata.Table;
import com.googlecode.paradox.metadata.paradox.ParadoxDataFile;
import com.googlecode.paradox.metadata.paradox.ParadoxTable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ParadoxHandler extends ParadoxData {


    public static void asJsonStream(OutputStream out,
                                    IRI resourceIRI,
                                    String tableNameCandidate,
                                    KeyValueStoreReadOnly contentStore) throws IOException {

        File tempFile = File.createTempFile("paradox", "db");

        try (OutputStream os = new FileOutputStream(tempFile)) {
            final InputStream inputStream = contentStore.get(resourceIRI);

            IOUtils.copyLarge(inputStream, os);

            ConnectionInfo connectionInfo = new ConnectionInfo("foo:bar");

            try {
                ParadoxDataFile data = loadHeader(tempFile, connectionInfo);
                if (data instanceof ParadoxTable) {
                    ParadoxTable table = (ParadoxTable) data;
                    table.setName(tableNameCandidate);
                    streamTable(out, resourceIRI, table);
                }
            } catch (SQLException var10) {
                connectionInfo.addWarning(var10);
            } catch (RuntimeException ex) {
                throw new IOException("failed to read [" + resourceIRI.getIRIString() + "]", ex);
            }


        } catch (IOException ex) {
            throw new IOException("failed to read [" + resourceIRI.getIRIString() + "]", ex);
        } finally {
            tempFile.delete();
        }

    }

    private static void streamTable(OutputStream out, IRI resourceIRI, ParadoxTable table) throws IOException {
        TableDataStream.streamData(table, table.getFields(), row -> {
            ObjectMapper obj = new ObjectMapper();
            ObjectNode objectNode = obj.createObjectNode();
            String iriString = "line:paradox:" + resourceIRI.getIRIString() + "!/" + table.getName() + "!/" + row.getLeft().toString();
            objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(iriString));
            objectNode.set("http://purl.org/dc/elements/1.1/format", TextNode.valueOf("application/paradox"));

            for (Pair<Field, Object> fieldObjectPair : row.getRight()) {
                if (fieldObjectPair.getRight() != null & fieldObjectPair.getLeft() != null) {
                    objectNode.put(fieldObjectPair.getKey().getName(), fieldObjectPair.getValue().toString());
                }

            }
            try {
                IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), out);
                IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), out);
            } catch (IOException e) {
                // ignore
            }
        });
    }
}
