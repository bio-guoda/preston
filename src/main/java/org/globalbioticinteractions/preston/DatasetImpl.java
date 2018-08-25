package org.globalbioticinteractions.preston;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public abstract class DatasetImpl implements Dataset {
    private final Dataset parent;
    private final DatasetType type;
    private String id;

    public DatasetImpl(Dataset parent, DatasetType type) {
        this.parent = parent;
        this.type = type;
    }

    public DatasetType getType() {
        return type;
    }

    public Dataset getParent() {
        return parent;
    }

    public String getId() { return id; }

    public void setId(String id) {
        this.id = id;
    }


}
