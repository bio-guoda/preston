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

import org.apache.commons.lang3.StringUtils;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveField;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.SimpleSaxHandler;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * SAX handler to parse a meta.xml descriptor for Darwin Core Archives. It populates a given archive instance and ignores
 * namespaces. The parser needs to be namespace aware!
 */
class MetaXMLSaxHandler2 extends SimpleSaxHandler {

    private static final TermFactory TERM_FACTORY = TermFactory.instance();
    private static final String NS_DWCA = "http://rs.tdwg.org/dwc/text/";

    private final Archive archive;
    private ArchiveFile af;

    MetaXMLSaxHandler2(Archive archive) {
        this.archive = archive;
    }

    private static Character getFirstChar(String x) throws UnsupportedArchiveException {
        if (x == null || x.length() == 0) {
            return null;
        }
        if (x.length() == 1) {
            return x.charAt(0);
        }
        if (x.equalsIgnoreCase("\\t")) {
            return '\t';
        }
        if (x.equalsIgnoreCase("\\n")) {
            return '\n';
        }
        if (x.equalsIgnoreCase("\\r")) {
            return '\r';
        }
        if (x.length() > 1) {
            throw new UnsupportedArchiveException(
                    "Only darwin core archives with a single quotation character are supported, but found >>>" + x + "<<<");
        }
        return ' ';
    }

    private static String unescapeBackslash(String x) {
        if (x == null || x.length() == 0) {
            return null;
        }
        return x.replaceAll("\\\\t", String.valueOf('\t')).replaceAll("\\\\n", String.valueOf('\n'))
                .replaceAll("\\\\r", String.valueOf('\r')).replaceAll("\\\\f", String.valueOf('\f'));
    }

    private ArchiveFile buildArchiveFile(Attributes attr) throws UnsupportedArchiveException {
        ArchiveFile dwcFile = new ArchiveFile();

        // extract the File attributes
        if (getAttr(attr, "encoding") != null) {
            dwcFile.setEncoding(getAttr(attr, "encoding"));
        }
        if (getAttrRaw(attr, "fieldsTerminatedBy") != null) {
            dwcFile.setFieldsTerminatedBy(unescapeBackslash(getAttrRaw(attr, "fieldsTerminatedBy")));
        }
        // for fieldsEnclosedBy there is a distinction between not provided and provided with an empty string
        if (getAttr(attr, "fieldsEnclosedBy") != null) {
            dwcFile.setFieldsEnclosedBy(getFirstChar(getAttr(attr, "fieldsEnclosedBy")));
        }
        else if (getAttrRaw(attr, "fieldsEnclosedBy") != null) {
            dwcFile.setFieldsEnclosedBy(null);
        }
        if (getAttrRaw(attr, "linesTerminatedBy") != null) {
            dwcFile.setLinesTerminatedBy(unescapeBackslash(getAttrRaw(attr, "linesTerminatedBy")));
        }
        if (getAttr(attr, "rowType") != null) {
            dwcFile.setRowType(TERM_FACTORY.findClassTerm(getAttr(attr, "rowType")));
        }
        String ignoreHeaderLines = getAttr(attr, "ignoreHeaderLines");
        try {
            dwcFile.setIgnoreHeaderLines(Integer.parseInt(ignoreHeaderLines));
        } catch (NumberFormatException ignored) { // swallow null or bad value
        }
        return dwcFile;
    }

    /**
     * Build an ArchiveField object based on xml attributes.
     */
    private ArchiveField buildField(Attributes attributes) {
        // build field
        Term term = TERM_FACTORY.findPropertyTerm(getAttr(attributes, "term"));
        String defaultValue = getAttr(attributes, "default");
        String vocabulary = getAttr(attributes, "vocabulary");
        ArchiveField.DataType type = ArchiveField.DataType.findByXmlSchemaType(getAttr(attributes, "type"));
        if (type == null) {
            type = ArchiveField.DataType.string;
        }
        String indexAsString = getAttr(attributes, "index");
        Integer index = null;
        if (indexAsString != null) {
            // let bad errors be thrown up
            try {
                index = Integer.parseInt(indexAsString);
            } catch (NumberFormatException e) {
                throw new UnsupportedArchiveException(e);
            }
        }
        String delimiter = getAttr(attributes, "delimitedBy");
        return new ArchiveField(index, term, defaultValue, type, delimiter, vocabulary);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        // calling the super method to stringify the character buffer
        super.endElement(uri, localName, qName);

        if (localName.equalsIgnoreCase("archive")) {
            // archive
        } else if (localName.equalsIgnoreCase("core")) {
            // update location to absolute path incl archive path
            //      makeLocationPathsAbsolute(af, archive.getLocation());
            archive.setCore(af);
        } else if (localName.equalsIgnoreCase("extension")) {
            // update location to absolute path incl archive path
            //      makeLocationPathsAbsolute(af, archive.getLocation());

            if (af.getId() != null && af.getId().getIndex() != null) {
                archive.addExtension(af);
            } else {
                log.warn("Skipping extension [" + af.getRowType() + "] with no index attribute");
            }
        } else if (localName.equalsIgnoreCase("location")) {
            // a file location
            af.addLocation(content);
        }

    }

    /**
     * Get attribute for the provided key.
     * This method will return null if the attribute is not found
     * or its value is an empty string.
     *
     * @param attributes
     * @param key
     *
     * @return attributes value or null
     */
    private static String getAttr(Attributes attributes, String key) {
        String result = getAttrRaw(attributes, key);
        return StringUtils.isBlank(result) ? null : result;
    }

    /**
     * Get attribute "raw" value for the provided key.
     * If the attribute is not found, null is returned.
     *
     * @param attributes
     * @param key
     *
     * @return attribute value or null if not found
     */
    private static String getAttrRaw(Attributes attributes, String key) {
        String val = null;
        if (attributes != null) {
            // try without NS
            val = attributes.getValue("", key);
            if (val == null) {
                // try with dwca NS if nothing found
                val = attributes.getValue(NS_DWCA, key);
            }
        }
        return val;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        if (localName.equalsIgnoreCase("archive") || localName.equalsIgnoreCase("stararchive")) {
            // metadata location
            archive.setMetadataLocation(getAttr(attributes, "metadata"));
        } else if (localName.equalsIgnoreCase("core") || localName.equalsIgnoreCase("extension")) {
            // archive/extension
            af = new ArchiveFile();
            if (localName.equalsIgnoreCase("core") || localName.equalsIgnoreCase("extension")) {
                // archive/core or archive/extension
                af = buildArchiveFile(attributes);
            }
        } else if (localName.equalsIgnoreCase("coreid") || localName.equalsIgnoreCase("id")) {
            ArchiveField field = buildField(attributes);
            if (af != null) {
                af.setId(field);
            } else {
                log.warn(localName + " field found outside of an archive file");
            }
        } else if (localName.equalsIgnoreCase("field")) {
            ArchiveField field = buildField(attributes);
            if (af != null) {
                af.addField(field);
            } else {
                log.warn("field found outside of an archive file");
            }
        }
    }
}
