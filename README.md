# Preston*: a biodiversity dataset tracker

[![Build Status](https://travis-ci.org/bio-guoda/preston.svg?branch=master)](https://travis-ci.org/bio-guoda/preston) [![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) 

 *Named after [Frank W. Preston (1896-1989)](https://en.wikipedia.org/wiki/Frank_W._Preston) and the Prestonian shortfall, one of the "[Seven Shortfalls that Beset Large-Scale Knowledge of Biodiversity](https://doi.org/10.1146/annurev-ecolsys-112414-054400)" as described by [Hortal et al. 2015](https://doi.org/10.1146/annurev-ecolsys-112414-054400). 

Preston is an open source software system that keeps track of biodiversity datasets. Scientists use Preston to keep a local, uniquely identifiable, versioned copy of (parts of) [GBIF](https://gbif.org). Institutions use Preston to check that their collections are indexed and available. Biodiversity informatics researchers use Preston to perform meta-data analysis. And finally, archivists can frugally distribute archives across the world. 
 
Preston uses the [PROV](https://www.w3.org/TR/prov-o/) and [PAV](https://pav-ontology.github.io/pav/) ontologies to model the actors, activities and entities involved in discovery, access and change control of digital biodiversity datasets. In addition, Preston implements content addressed storage and SHA256 hashes to uniquely identify and store content. A [hexastore](https://people.csail.mit.edu/tdanford/6830papers/weiss-hexastore.pdf)-like index is used to navigate a (local) graph of biodiversity datasets. Preston is designed to work offline and can be cloned, copied and moved across storage media with existing tools and infrastructures like rsync, dropbox, the internet archive or thumbdrives. In addition to versioned copies of uniquely identifiable original [ABCD](http://tdwg.github.io/abcd/ABCD_v206.html)-A, [DWC](http://rs.tdwg.org/dwc/)-A and [EML](https://www.researchgate.net/profile/Oliver_Guenther/publication/228958840_EML-the_Environmental_Markup_Language/links/0046351ee4c535bf56000000.pdf?inViewer=true&disableCoverPage=true&origin=publication_detail) files, Preston also keeps track of the [GBIF](https://gbif.org), [iDigBio](https://idigbio.org) and [BioCASe](http://biocasemonitor.biodiv.naturkundemuseum-berlin.de/index.php/Main_Page) registries to help retain the relationships between the institutions to keep a detailed record of provenance. 

If you haven't yet tried preston, please see the [Installation](#install) section.

## Table of Contents
 
 * [Usage](#usage) - command available on the preston commandline tool
   * [Command Line Tool](#command-line-tool)
      * [`update`](#update) - update biodiversity graph
      * [`ls`](#ls) - list/print biodiversity graph
      * [`get`](#get) - print biodiversity graph node (e.g., dwca)
      * [`history`](#history) - show history of biodiversity graph node
   * [Use Cases](#use-cases)
      * [`retrieve citations`](#retrieve-citations)
      * [`archiving`](#archiving)
      * [`data-access-monitor`](#data-access-monitor)
      * [`compare-versions`](#compare-versions)
      * [`mirroring`](#mirroring)
 * [Prerequisites](#prerequisites)
 * [Install](#install)
 * [Building](#building)
 * [Contribute](#contribute)
 * [License](#license)

## Usage

Preston was designed with the [unix philosophy](https://en.wikipedia.org/wiki/Unix_philosophy) in mind: a simple tools with a specific focus. For Preston, the focus is keeping track of biodiversity archives available through registries like [GBIF](https://gbif.org), [iDigBio](https://idigbio.org) and [BioCASe](http://biocasemonitor.biodiv.naturkundemuseum-berlin.de/index.php/Main_Page). The functionality is currently available through a command line tool.

### Command Line Tool

The command line tool provides four commands: ```update```, ```ls```, ```get``` and ```history```. In short, the commands are used to track and access DwC-A, EMLs and various registries. The output of the tools is nquads or tsv. Both output formats are structured in three "columns" to form a three term sentence per line. In a way, this output is telling you the story of your local biodiversity data graph in terms of simple sentences. This line-by-line format helps to re-use existing text processing tools like awk, sed, cut, etc. Also, tab-separated-values output plays well with spreadsheet applications and [R](https://r-project.org).

The examples below assume that you've created a shortcut ```preston``` to ```java -jar preston.jar ``` (see [installation](#installation)).

#### `update`

The ```update``` command updates your local biodiversity graph using remote resources. By default, Preson uses GBIF, iDigBio and BioCASe and retrieve associated registries and data archives. The output are statements, expressed in nquads (or nquad-like tsv). In depth discussion of rdf, nquads and related topics are beyond the current scope. However, with a little patience, you can probably disect that Preston is trying to communicate. 

For instance:

```console
preston update 
$ preston update
<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .
<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Agent> .
<https://preston.guoda.org> <http://purl.org/dc/terms/description> "Preston is a software program that finds, archives and provides access to biodiversity datasets."@en .
<0b472626-1ef2-4c84-ab8f-9e455f7b6bb6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Activity> .
<0b472626-1ef2-4c84-ab8f-9e455f7b6bb6> <http://purl.org/dc/terms/description> "A crawl event that discovers biodiversity archives."@en .
<0b472626-1ef2-4c84-ab8f-9e455f7b6bb6> <http://www.w3.org/ns/prov#startedAtTime> "2018-09-05T04:42:40.108Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<0b472626-1ef2-4c84-ab8f-9e455f7b6bb6> <http://www.w3.org/ns/prov#wasStartedBy> <https://preston.guoda.org> .
<0659a54f-b713-4f86-a917-5be166a14110> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Entity> .
...
```

Tells us that there's a software program called "Preston" that started a crawl on 2018-09-05 . 


      * [`update`](#update) - update biodiversity graph
      * [`ls`](#ls) - list/print biodiversity graph
      * [`get`](#get) - print biodiversity graph node (e.g., dwca)
      * [`history`](#history) - show history of biodiversity graph node
   * [Use Cases](#use-cases)
      * [`retrieve citations`](#retrieve-citations)
      * [`archiving`](#archiving)
      * [`data-access-monitor`](#data-access-monitor)
      * [`compare-versions`](#compare-versions)
      * [`mirroring`](#mirroring)


## Prerequisites

Preston needs Java 8+.

## Install

Preston is a stand-alone java application, packaged in a jarfile. You can build you own (see [building](#building)) or download a prebuilt jar at [releases](https://github.com/bio-guoda/preston/releases).

On linux (and Mac) it is recommended to make an alias by appending the following to ~/.bash_aliases :

```
alias preston='java -Xmx4G -jar [some dir]/preston.jar'
```

where [some dir] is the location where preston.jar lives. With this alias, you can now do ```preston version``` instead of ```java -jar preston.jar version```.

### Building

Please use [maven](https://maven.apache.org) version 3.3+.

* Clone this repository
* Run tests using `mvn test` (optional).
* Run `mvn package -DskipTests` to build (standalone) jar
* Copy `preston/target/preston-[version]-jar-with-dependencies.jar` to ```[some dir]/preston.jar```

## Examples

### preston update 



### Maven, Gradle, SBT
Preston is made available through a [maven](https://maven.apache.org) repository.

To include ```preston``` in your project, add the following sections to your pom.xml (or equivalent for sbt, gradle etc):
```
  <repositories>
    <repository>
        <id>depot.globalbioticinteractions.org</id>
        <url>https://depot.globalbioticinteractions.org/release</url>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>bio.guoda</groupId>
      <artifactId>preston</artifactId>
      <version>0.0.1</version>
    </dependency>
  </dependencies>
```

## Usage

```
Usage: <main class> [command] [command options]
  Commands:
    ls      list biodiversity graph
      Usage: ls [options]
        Options:
          -l, --log
            log format
            Default: nquads
            Possible Values: [tsv, nquads, dots]

    get      get biodiversity node(s)
      Usage: get node id (e.g., [hash://sha256/8ed311...])

    update      update biodiversity graph
      Usage: update [options] content URLs to update. If specified, the seeds
            will not be used.
        Options:
          -i, --incremental
            resume unfinished update
            Default: false
          -l, --log
            log format
            Default: nquads
            Possible Values: [tsv, nquads, dots]
          -u, --seed-uris
            starting points for graph discovery. Only active when no content
            urls are provided.
            Default: [https://idigbio.org, https://gbif.org, http://biocase.org]

    history      show history of biodiversity resource
      Usage: history [options] biodiversity resource locator
        Options:
          -l, --log
            log format
            Default: nquads
            Possible Values: [tsv, nquads, dots]

    version      show version
      Usage: version
```


## Contribute
Feel free to join in. All welcome. Open an [issue](https://github.com/bio-guoda/preston/issues)!

## License

[MIT](LICENSE)
