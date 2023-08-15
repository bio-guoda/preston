package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

import java.util.Arrays;
import java.util.List;

public final class Seeds {

    public final static IRI DATA_ONE = RefNodeFactory.toIRI("https://dataone.org");
    public final static IRI GBIF = RefNodeFactory.toIRI("https://gbif.org");
    public final static IRI TAXONWORKS = RefNodeFactory.toIRI("https://sfg.taxonworks.org");
    public final static IRI CHECKLIST_BANK = RefNodeFactory.toIRI("https://checklistbank.org");
    public final static IRI BHL = RefNodeFactory.toIRI("https://biodiversitylibrary.org");
    public final static IRI ALA = RefNodeFactory.toIRI("https://ala.org.au");
    public final static IRI IDIGBIO = RefNodeFactory.toIRI("https://idigbio.org");
    public final static IRI BIOCASE = RefNodeFactory.toIRI("http://biocase.org");
    public final static IRI OBIS = RefNodeFactory.toIRI("https://obis.org");

    public final static List<IRI> AVAILABLE
            = Arrays.asList(GBIF, IDIGBIO, BIOCASE, DATA_ONE, BHL, OBIS, ALA, TAXONWORKS);
}
