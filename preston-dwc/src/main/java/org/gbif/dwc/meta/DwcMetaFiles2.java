/*
 * Copyright 2021 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.dwc.meta;

import org.apache.commons.io.input.BOMInputStream;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Collections of static methods to work with Metadata files (e.g. metadata.xml, eml.xml) in
 * the context of Darwin Core (archive) files.
 */
public class DwcMetaFiles2 {

    private static final SAXParserFactory SAX_FACTORY = SAXParserFactory.newInstance();
    static {
        SAX_FACTORY.setNamespaceAware(true);
        SAX_FACTORY.setValidating(false);
    }

    //common filenames used for the metadata file
    private static final List<String> POSSIBLE_METADATA_FILE =
            Collections.unmodifiableList(Arrays.asList("eml.xml", "metadata.xml"));

    private DwcMetaFiles2(){}

    /**
     * Read the provided meta descriptor (e.g. meta.xml) and return a {@link Archive}.
     * @param metaDescriptor
     * @throws SAXException
     * @throws IOException
     * @throws UnsupportedArchiveException
     * @return a new {@link Archive}, never null
     */
    public static Archive fromMetaDescriptor(InputStream metaDescriptor) throws SAXException, IOException, UnsupportedArchiveException {
        Archive archive = new Archive();
        try (BOMInputStream bomInputStream = BOMInputStream.builder().setInputStream(metaDescriptor).get()) {
            SAXParser p = SAX_FACTORY.newSAXParser();
            MetaXMLSaxHandler2 mh = new MetaXMLSaxHandler2(archive);
            p.parse(bomInputStream, mh);
        } catch (ParserConfigurationException e) {
            throw new SAXException(e);
        }
        return archive;
    }

}
