# Preston*: a biodiversity dataset tracker

[![Build Status](https://travis-ci.org/bio-guoda/preston.svg?branch=master)](https://travis-ci.org/bio-guoda/preston) [![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) 

 *Named after [Frank W. Preston (1896-1989)](https://en.wikipedia.org/wiki/Frank_W._Preston) and the Prestonian shortfall, one of the "[Seven Shortfalls that Beset Large-Scale Knowledge of Biodiversity](https://doi.org/10.1146/annurev-ecolsys-112414-054400)" as described by [Hortal et al. 2015](https://doi.org/10.1146/annurev-ecolsys-112414-054400). 

Preston is an open-source software system that keeps track of biodiversity datasets. Scientists can use Preston to keep a local, uniquely identifiable, versioned copy of all or parts of [GBIF](https://gbif.org)-indexed datasets. Institutions can use Preston to check that their collections are indexed and available. Biodiversity informatics researchers can use Preston to perform metadata analysis. And finally, archivists can distribute archives across the world. 
 
Preston uses the [PROV](https://www.w3.org/TR/prov-o/) and [PAV](https://pav-ontology.github.io/pav/) ontologies to model the actors, activities and entities involved in discovery, access and change control of digital biodiversity datasets. In addition, Preston uses [content-addressed storage](https://bentrask.com/?q=hash://sha256/98493caa8b37eaa26343bbf73f232597a3ccda20498563327a4c3713821df892) and [SHA256 hashes](https://en.wikipedia.org/wiki/SHA-2) to uniquely identify and store content. A [hexastore](https://people.csail.mit.edu/tdanford/6830papers/weiss-hexastore.pdf)-like index is used to navigate a local graph of biodiversity datasets. Preston is designed to work offline and can be cloned, copied and moved across storage media with existing tools and infrastructures like rsync, dropbox, the internet archive or thumbdrives. In addition to versioned copies of uniquely identifiable original [ABCD](http://tdwg.github.io/abcd/ABCD_v206.html)-A, [DWC](http://rs.tdwg.org/dwc/)-A and [EML](https://www.researchgate.net/profile/Oliver_Guenther/publication/228958840_EML-the_Environmental_Markup_Language/links/0046351ee4c535bf56000000.pdf?inViewer=true&disableCoverPage=true&origin=publication_detail) files, Preston also keeps track of the [GBIF](https://gbif.org), [iDigBio](https://idigbio.org) and [BioCASe](http://biocasemonitor.biodiv.naturkundemuseum-berlin.de/index.php/Main_Page) registries to help retain the relationships between the institutions to keep a detailed record of provenance. 

If you haven't yet tried preston, please see the [Installation](#install) section. Please also see [a template repository](https://github.com/bio-guoda/preston-amazon) and [use cases](#use-cases) for examples.

## Table of Contents
 
 * [Usage](#usage) - command available on the preston commandline tool
   * [Command Line Tool](#command-line-tool)
      * [`update`](#update) - update biodiversity graph
      * [`ls`](#ls) - list/print biodiversity graph
      * [`get`](#get) - print biodiversity graph node (e.g., dwca)
      * [`history`](#history) - show history of biodiversity graph node
   * [Use Cases](#use-cases)
      * [`mining citations`](#mining-citations)
      * [`archiving`](#archiving)
      * [`data access monitor`](#data-access-monitor)
      * [`compare versions`](#compare-versions)
      * [`generating citations`](#generating-citations)
 * [Prerequisites](#prerequisites)
 * [Install](#install)
 * [Building](#building)
 * [Contribute](#contribute)
 * [License](#license)

## Usage

Preston was designed with the [unix philosophy](https://en.wikipedia.org/wiki/Unix_philosophy) in mind: a simple tool with a specific focus that works well with others. For Preston, the focus is keeping track of biodiversity archives available through registries like [GBIF](https://gbif.org), [iDigBio](https://idigbio.org) and [BioCASe](http://biocasemonitor.biodiv.naturkundemuseum-berlin.de/index.php/Main_Page). The functionality is currently available through a command line tool.

### Command Line Tool

The command line tool provides four commands: ```update```, ```ls```, ```get``` and ```history```. In short, the commands are used to track and access DwC-A, EMLs and various registries. The output of the tools is [nquads](https://www.w3.org/TR/n-quads/) or [tsv](https://www.iana.org/assignments/media-types/text/tab-separated-values). Both output formats are structured in "columns" to form a three term sentence per line. In a way, this output is telling you the story of your local biodiversity data graph in terms of simple sentences. This line-by-line format helps to re-use existing text processing tools like awk, sed, cut, etc. Also, tab-separated-values output plays well with spreadsheet applications and [R](https://r-project.org).

The examples below assume that you've created a shortcut ```preston``` to ```java -jar preston.jar ``` (see [installation](#install)).

#### `update`

The ```update``` command updates your local biodiversity graph using remote resources. By default, Preston uses GBIF, iDigBio and BioCASe to retrieve associated registries and data archives. The output is statements, expressed in nquads (or nquad-like tsv). An in depth discussion of rdf, nquads and related topics are beyond the current scope. However, with a little patience, you can probably figure out what Preston is trying to communicate. 

For instance:

```console
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

tells us that there's a software program called "Preston" that started a crawl on 2018-09-05 . A little farther down, you'll see things like:

```console
<https://gbif.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Organization> .
<https://api.gbif.org/v1/dataset> <http://purl.org/dc/terms/description> "Provides a registry of Darwin Core archives, and EML descriptors."@en .
<https://api.gbif.org/v1/dataset> <http://purl.org/pav/createdBy> <https://gbif.org> .
<https://api.gbif.org/v1/dataset> <http://purl.org/dc/elements/1.1/format> "application/json" .
<https://api.gbif.org/v1/dataset> <http://purl.org/pav/hasVersion> <hash://sha256/5d1bb4f3a5a9da63fc76efc4d7b4a7debbec954bfd056544225c294fff679b4c> .
```

which says that GBIF, an organization created a registry that has a version at <hash://sha256/5d1bb4f3a5a9da63fc76efc4d7b4a7debbec954bfd056544225c294fff679b4c> . This weird looking url is a [content-addressed hash](https://bentrask.com/?q=hash://sha256/98493caa8b37eaa26343bbf73f232597a3ccda20498563327a4c3713821df892). Rather than describing where things are (e.g., https://eol.org), content-addressed hashes describe what they contain. 

If you don't want to download the entire biodiversity graph (~60GB) onto your computer, you can also use [GBIF's dataset registry search api](https://www.gbif.org/developer/registry) as a starting point. For instance, if you run ```preston update "http://api.gbif.org/v1/dataset/suggest?q=Amazon&amp;type=OCCURRENCE"```, you only get occurence datasets that GBIF suggests are related to the Amazon. If you track these suggested datasets, you might see something like:

```console
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/dc/elements/1.1/format> "application/dwca" .
<hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-05T05:11:33.592Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> <http://www.w3.org/ns/prov#wasGeneratedBy> <21de25a8-927f-49a1-99be-725f1f506232> .
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/pav/hasVersion> <hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> .
```

which tells us that a [darwin core archive](http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip) was found and a copy of it was made on 2018-09-05. The copy, or version, has a content hash of hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8 . Incidentally, you can reach this same exact dataset at [web-accessible preston archive](https://preston.guoda.bio/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8). With this, we established that on 2018-09-05 a specific web addressed produced a specific content. On the next update run, Preston will download the content again. If the content is the same as before, nothing happens. If the content changed, a new version will be created associated with the same address, establishing a versioning of the content produced by the web address. This is addressed in a statement like ```<some hash> <.../previousVersion> <some previous hash>```. 

So, in a nutshell, the update process produces a detailed record of which resources are downloaded, what they look like and were they came from. You can retrieve the record of a successful run by using `ls`.

#### `ls`  

`ls` print the results of the previous updates. An update always refers to a previous update, so that a complete history can be printed / replayed of all past updates. So, the `ls` commands lists your (local) copy of the biodiversity graph. 

#### `get`

`get` retrieves a specific node in the biodiversity graph. This can be a darwin core archive, EML file but also a copy of the iDigBio publisher registry. For instance, if you'd like to retrieve the node with DwC-A content, get the file and list the content using ```unzip``` and access the references in the taxa.txt file.

```console
$ preston get hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8 > dwca.zip 
$ unzip -l dwca.zip 
Archive:  bla.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
    11694  2016-01-03 13:36   meta.xml
     4664  2016-01-03 13:36   eml.xml
     5533  2017-06-20 02:39   taxa.txt
      284  2017-06-20 02:39   occurrences.txt
    16978  2017-06-20 02:39   description.txt
       54  2017-06-20 02:39   distribution.txt
    48439  2017-06-20 02:39   media.txt
     9280  2017-06-20 02:39   references.txt
       33  2017-06-20 02:39   vernaculars.txt
---------                     -------
    96959                     9 files
$ unzip -p dwca.zip taxa.txt | cut -f16
references
http://treatment.plazi.org/id/038787F9FE149009FED7FE39FEA9FCEE
http://treatment.plazi.org/id/038787F9FE1B9004FED7FA49FE99FA06
http://treatment.plazi.org/id/038787F9FE1E9002FED7FA5EFC1FFE3E
```

The implication of using content addressed storage is that if the hash is the same, you are guaranteed that the content is the same. So, you can reproduce the **exact** same results above if you have a file with the same content hash.


#### History

History helps to list your local content versions associated with a web address. Because the internet today might not be the internet of yesterday, and because publishers update their content for various reasons, Preston helps you keep track of the different versions retrieved from a particular location. Just like the [Internet Archive](https://archive.org)'s Way Back Machine keeps track of web page content, Preston help you keep track of the datasets that you are interested in. 

To inspect the history you can type:

```console
$ preston history
<0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/ea430cf506640ffd170d88bfc429c979d9e8ded97d839f17fdf9f4d8227017c2> .
<hash://sha256/0231077876124b92cc001f3c19651b536fa10a15fd94bfb7912e60722f2bde1d> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-05T02:24:43.730Z" .
<hash://sha256/0231077876124b92cc001f3c19651b536fa10a15fd94bfb7912e60722f2bde1d> <http://purl.org/pav/previousVersion> <hash://sha256/ea430cf506640ffd170d88bfc429c979d9e8ded97d839f17fdf9f4d8227017c2> .
<hash://sha256/c00b87e43b8b5a63ee68d4057138df342ae4f709cc794a74bed3ed0a1ccbdd7b> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-05T02:34:49.282Z" .
<hash://sha256/c00b87e43b8b5a63ee68d4057138df342ae4f709cc794a74bed3ed0a1ccbdd7b> <http://purl.org/pav/previousVersion> <hash://sha256/0231077876124b92cc001f3c19651b536fa10a15fd94bfb7912e60722f2bde1d> .
...
``` 

By default, the `history` command shows the versions of your local biodiversity graph as a **whole**. A list of versions associated with the sequence of updates. If you'd like to know what the UUID 0659a54f-b713-4f86-a917-5be166a14110 is described as, you can use `ls` and filter by the UUID:

```console
$ preston ls | grep 0659a54f-b713-4f86-a917-5be166a14110
<0659a54f-b713-4f86-a917-5be166a14110> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Entity> .
<0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/dc/terms/description> "A biodiversity graph archive."@en .
```

So, the UUID ending on 4110 is describe as "A biodiversity graph archive". This UUID is the same across all Preston updates, so in a way we are help to create different versions of the same "a biodiversity graph". Good to know right? 

You can also use `history` for a specific url, like:

```console
$ preston history http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/pav/hasVersion> <hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> .
```

### Use Cases

In the previous section the commands `update`, `ls`, `get` and `history` were introduced. Now, some use cases are covered to show how to combine these basic commands to make for useful operations. This is by no means an exhaustive list of all the potential uses, but instead is just to provide some inspiration on how to get the most out of preston.

#### Mining Citations

The Ecological Metadata Language (EML) files contain citations, and your biodiversity graph contains EML files. To extract all citations you can do:

```console
# first make a list of all the emls
preston ls | grep application/eml | cut -f1 > emls.txt
# then 
preston ls -l tsv | grep -f emls.tsv | grep "Version" | grep hash | cut -f3 | preston get | grep citation | sed 's/<[^>]*>//g' > citations.txt
head citations.txt
            HW Jackson C, Ochieng J, Musila S, Navarro R, Kanga E (2018): A Checklist of the Mammals of Arabuko-Sokoke Forest, Kenya, 2018. v1.0. A Rocha Kenya. Dataset/Checklist. http://ipt.museums.or.ke/ipt/resource?r=asfmammals&amp;v=1.0
            Adda M., Sanou L., Bori H., 2018. Specimens d&apos;herbier de la flore du Niger. Données d&apos;occurrence publiées dans le cadre du Prjet BID Régional. CNSF-Niger  					
                Michel.C., 2000. Arbres,arbustes et lianes des zones sèches d&apos;Afrique de l&apos;Ouest.3ème édition.Quae.MNHN.573p.
            Hendrickson D A, Cohen A, Casarez M (2018): Ichthyology. v1.3. University of Texas at Austin, Biodiversity Collections. Dataset/Occurrence. http://ipt.tacc.utexas.edu/resource?r=tnhci&amp;v=1.3
            Urrutia N S (2014): Caracterización Florística de un Área Degradada por Actividad Minera en la Costa Caucana. v2.0. Instituto de Investigaciones Ambientales del Pacifico John Von Neumann (IIAP). Dataset/Occurrence. http://doi.org/10.15472/mkjqef

```
So, now we have a way to attribute each and every dataset individually.

#### Archiving

Preston creates a "data" folder that stores the biodiversity datasets and associated information. For archiving, you can take this "data" folder, copy it and move it somewhere safe. You can also use tools like [git-annex](http://git-annex.branchable.com), [rsync](https://en.wikipedia.org/wiki/Rsync), or use distributed storage systems like the [Interplanetary File System (ipfs)](https://ipfs.io) or [Dat](https://dat-project.org). 

#### Data Access Monitor

By running [`update`](#update) periodically and checking for "blank", or "missing" nodes (see [blank skolemization](https://www.w3.org/TR/rdf11-concepts/#section-skolemization)), you can make a list of the dataset providers that went offline or are not responding.

The example below wsa created on 2018-09-05 using biodiversity dataset graph with hash [hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93](https://preston.guoda.bio/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93).

```console
$ preston ls -l tsv | grep "/.well-known/genid/" | grep "Version" | cut -f1,3 | tr '\t' '\n' | grep -v "/.well-known/genid/" | grep -v "hash" | sort | uniq -c | sort -nr | head -n10
     21 http://187.32.44.123/ipt/eml.do?r=camporupestre-15plot-survey-sampling-itacolomi-lagoa076-checklist
     21 http://187.32.44.123/ipt/eml.do?r=camporupestre-15plot-survey-sampling-itacolomi-calais107-checklist
     12 http://187.32.44.123/ipt/eml.do?r=2014-10-12-ufv-leep-buriti-543b4b1b47b42
      6 http://xbiod.osu.edu/ipt/eml.do?r=rome
      6 http://xbiod.osu.edu/ipt/eml.do?r=osuc
      6 http://xbiod.osu.edu/ipt/archive.do?r=rome
      6 http://91.151.189.38:8080/viript/eml.do?r=Avena_herbarium1
      3 http://xbiod.osu.edu/ipt/eml.do?r=ucfc
      3 http://xbiod.osu.edu/ipt/eml.do?r=proctos
      3 http://xbiod.osu.edu/ipt/eml.do?r=platys
```
 
#### Compare Versions

Keeping track of changes across a diverse consortium of data publishers is necessary for reproducible workflows and reliable results. As datasets change, Preston can help you give insights into what changed *exactly*. For instance, the GBIF dataset registry changes as datasets are added, updated or deprecated. Below is an example of two version of the  https://api.gbif.org/v1/dataset endpoint, one from 2018-09-03 and the other from 2018-09-04. Using ```jq``` and ```diff``` in combination with ```preston get``` and ```preston history``` gives us a way to check and see what changed.

```console
$ preston history https://api.gbif.org/v1/dataset
<https://api.gbif.org/v1/dataset> <http://purl.org/pav/hasVersion> <hash://sha256/184886cc6ae4490a49a70b6fd9a3e1dfafce433fc8e3d022c89e0b75ea3cda0b> .
<hash://sha256/1846abf2b9623697cf9b2212e019bc1f6dc4a20da51b3b5629bfb964dc808c02> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-03T02:19:14.636Z" .
<hash://sha256/1846abf2b9623697cf9b2212e019bc1f6dc4a20da51b3b5629bfb964dc808c02> <http://purl.org/pav/previousVersion> <hash://sha256/184886cc6ae4490a49a70b6fd9a3e1dfafce433fc8e3d022c89e0b75ea3cda0b> .
$ preston get hash://sha256/184886cc6ae4490a49a70b6fd9a3e1dfafce433fc8e3d022c89e0b75ea3cda0b | jq . > one.json
$ preston get hash://sha256/1846abf2b9623697cf9b2212e019bc1f6dc4a20da51b3b5629bfb964dc808c02 | jq . > two.json
$ diff one.json two.json
20c20
<         "text": "Ali P A, Maddison W P, Zahid M, Butt A (2017). New chrysilline and aelurilline jumping spiders from Pakistan (Araneae, Salticidae). Plazi.org taxonomic treatments database. Checklist dataset https://doi.org/10.3897/zookeys.783.21985 accessed via GBIF.org on 2018-08-31."
---
>         "text": "Ali P A, Maddison W P, Zahid M, Butt A (2017). New chrysilline and aelurilline jumping spiders from Pakistan (Araneae, Salticidae). Plazi.org taxonomic treatments database. Checklist dataset https://doi.org/10.3897/zookeys.783.21985 accessed via GBIF.org on 2018-09-03."
248c248
```


#### Generating Citations

Preston provides both a date and a content-based identifier for the datasets that you are using and the biodiversity graph as a whole. Also, it produces the information is a format that is machine readable. This supports the automated generation of citations, for human or machine consumption, as evidenced by the reference to a [particular version of the biodiversity dataset graph](https://preston.guoda.bio/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93) in the previous section. 

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
