package org.globalbioticinteractions.preston;

import java.io.IOException;
import java.io.InputStream;

public interface Dataset {

    Dataset getParent();

    DatasetType getType();

    InputStream getData() throws IOException;

    String getLabel();

    String getId();

}

