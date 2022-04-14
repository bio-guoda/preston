package bio.guoda.preston.store;

import org.apache.commons.lang3.StringEscapeUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class XMLEscapingTest {

    // related to:
    // $ preston cat 'zip:hash://sha256/d47fa5353f0a5a78ac0eb74db37f89f1ac255b8065f75bc35bc124c62047035c!/meta.xml'
    //<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    //<archive metadata="eml.xml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://rs.tdwg.org/dwc/text/">
    //    <core encoding="UTF-8" fieldsEnclosedBy='"' fieldsTerminatedBy="&#x9;" linesTerminatedBy="&#xA;" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
    //        <files>
    //            <location>occurrence.txt</location>
    //        </files>
    //        <id index="0"/>
    //        <field index="1" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
    //        <field index="2" term="http://rs.tdwg.org/dwc/terms/catalogNumber"/>
    //        <field index="3" term="http://rs.tdwg.org/dwc/terms/collectionCode"/>
    //        <field index="4" term="http://rs.tdwg.org/dwc/terms/continent"/>
    //		<field index="5" term="http://rs.tdwg.org/dwc/terms/higherGeography"/>
    //        <field index="6" term="http://rs.tdwg.org/dwc/terms/country"/>
    //        <field index="7" term="http://rs.tdwg.org/dwc/terms/stateProvince"/>
    //		<field index="8" term="http://rs.tdwg.org/dwc/terms/locality"/>
    //		<field index="9" term="http://rs.tdwg.org/dwc/terms/verbatimEventDate"/>
    //		<field index="10" term="http://rs.tdwg.org/dwc/terms/eventDate"/>
    //	    <field index="11" term="http://rs.tdwg.org/dwc/terms/kingdom"/>
    //	    <field index="12" term="http://rs.tdwg.org/dwc/terms/family"/>
    //	    <field index="13" term="http://rs.tdwg.org/dwc/terms/genus"/>
    //	    <field index="14" term="http://rs.tdwg.org/dwc/terms/subgenus"/>
    //        <field index="15" term="http://rs.tdwg.org/dwc/terms/specificEpithet"/>
    //	    <field index="16" term="http://rs.tdwg.org/dwc/terms/infraspecificEpithet"/>
    //        <field index="17" term="http://rs.tdwg.org/dwc/terms/verbatimTaxonRank"/>
    //        <field index="18" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship"/>
    //		<field index="19" term="http://rs.tdwg.org/dwc/terms/scientificName"/>
    //        <field index="20" term="http://rs.tdwg.org/dwc/terms/typeStatus"/>
    //        <field index="21" term="http://rs.tdwg.org/dwc/terms/individualCount"/>
    //        <field index="22" term="http://rs.tdwg.org/dwc/terms/preparations"/>
    //        <field index="23" term="http://rs.tdwg.org/dwc/terms/recordedBy"/>
    //        <field index="24" term="http://rs.tdwg.org/dwc/terms/sex"/>
    //		<field index="25" term="http://rs.tdwg.org/dwc/terms/lifeStage"/>
    //        <field index="26" term="http://rs.tdwg.org/dwc/terms/occurrenceID"/>
    //        <field index="27" term="http://rs.tdwg.org/dwc/terms/nomenclaturalCode"/>
    //        <field index="28" term="http://rs.tdwg.org/dwc/terms/institutionCode"/>
    //    </core>
    //</archive>

    @Test
    public void escapedTab() {
        String s = StringEscapeUtils.unescapeXml("&#x9;");
        assertThat(s, Is.is("\t"));
    }

    @Test
    public void escapedNewline() {
        String s = StringEscapeUtils.unescapeXml("&#xA;");
        assertThat(s, Is.is("\n"));
    }
}
