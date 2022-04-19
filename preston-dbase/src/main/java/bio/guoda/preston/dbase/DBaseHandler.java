package bio.guoda.preston.dbase;

import bio.guoda.preston.store.KeyValueStoreReadOnly;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DBaseHandler {


    public static void asJsonStream(OutputStream out, IRI resourceIRI, KeyValueStoreReadOnly contentStore) throws IOException {

        try {
            DBFReader reader = new DBFReader(contentStore.get(resourceIRI));

            List<String> header = new ArrayList<>();
            for (int i = 0; i < reader.getFieldCount(); i++) {
                DBFField field = reader.getField(i);
                header.add(field.getName());
            }

            Object[] rowObjects;
            while ((rowObjects = reader.nextRecord()) != null) {
                ObjectMapper obj = new ObjectMapper();
                ObjectNode objectNode = obj.createObjectNode();
                for (int i = 0; i < rowObjects.length; i++) {
                    if (i > header.size() - 1) {
                        throw new IOException("found record with more fields [" + rowObjects.length + "] than were defined in header [" + header.size() + "] of [" + resourceIRI.getIRIString() + "]");
                    }
                    String fieldName = header.get(i);
                    Object rowObject = rowObjects[i];
                    if (i == 0) {
                        objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf(resourceIRI.getIRIString()));
                        objectNode.set("http://purl.org/dc/elements/1.1/format", TextNode.valueOf("application/dbase"));
                    }
                    objectNode.put(fieldName, rowObject == null ? "" : rowObject.toString());
                }
                if (objectNode.size() > 0) {
                    IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), out);
                    IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), out);
                }
            }
        } catch (DBFException ex) {
            // ignore dbf related parsing issues:
            // do opportunistic handling so that non-DBF files are ignored.
        }

    }
}
