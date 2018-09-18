# Preston*: a biodiversity dataset tracker

[![Build Status](https://travis-ci.org/bio-guoda/preston.svg?branch=master)](https://travis-ci.org/bio-guoda/preston) [![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1410544.svg)](https://doi.org/10.5281/zenodo.1410544)

 *Named after [Frank W. Preston (1896-1989)](https://en.wikipedia.org/wiki/Frank_W._Preston) and the Prestonian shortfall, one of the "[Seven Shortfalls that Beset Large-Scale Knowledge of Biodiversity](https://doi.org/10.1146/annurev-ecolsys-112414-054400)" as described by [Hortal et al. 2015](https://doi.org/10.1146/annurev-ecolsys-112414-054400). 

Preston is an open-source software system that keeps track of biodiversity datasets. Scientists can use Preston to keep a local, uniquely identifiable, versioned copy of all or parts of [GBIF](https://gbif.org)-indexed datasets. Institutions can use Preston to check that their collections are indexed and available through [iDigBio](https://idigbio.org). Biodiversity informatics researchers can use Preston to perform metadata analysis. And finally, archivists can distribute archives across the world. 
 
Preston uses the [PROV](https://www.w3.org/TR/prov-o/) and [PAV](https://pav-ontology.github.io/pav/) ontologies to model the actors, activities and entities involved in discovery, access and change control of digital biodiversity datasets. In addition, Preston uses [content-addressed storage](https://bentrask.com/?q=hash://sha256/98493caa8b37eaa26343bbf73f232597a3ccda20498563327a4c3713821df892) and [SHA256 hashes](https://en.wikipedia.org/wiki/SHA-2) to uniquely identify and store content. A [hexastore](https://people.csail.mit.edu/tdanford/6830papers/weiss-hexastore.pdf)-like index is used to navigate a local graph of biodiversity datasets. Preston is designed to work offline and can be cloned, copied and moved across storage media with existing tools and infrastructures like rsync, dropbox, the internet archive or thumbdrives. In addition to versioned copies of uniquely identifiable original [ABCD](http://tdwg.github.io/abcd/ABCD_v206.html)-A, [DWC](http://rs.tdwg.org/dwc/)-A and [EML](https://www.researchgate.net/profile/Oliver_Guenther/publication/228958840_EML-the_Environmental_Markup_Language/links/0046351ee4c535bf56000000.pdf?inViewer=true&disableCoverPage=true&origin=publication_detail) files, Preston also keeps track of the [GBIF](https://gbif.org), [iDigBio](https://idigbio.org) and [BioCASe](http://biocasemonitor.biodiv.naturkundemuseum-berlin.de/index.php/Main_Page) registries to help retain the relationships between the institutions to keep a detailed record of provenance. 

The process diagram below shows how Preston starts crawls to create versioned copies registries and datasets. The statements used to describe this process are then used to generate an update version of a biodiversity dataset graph. The numbers indicate the sequence of events. Click on the image to enlarge. 

<img src="https://raw.githubusercontent.com/bio-guoda/preston/master/process.png" width="50%">

If you haven't yet tried Preston, please see the [Installation](#install) section. Please also see [a template repository](https://github.com/bio-guoda/preston-amazon) and [use cases](#use-cases) for examples. If you are interested in learning how Preston works, please visit the [architecture](./architecture.md) page.

## Table of Contents
 
 * [Usage](#usage) - command available on the preston commandline tool
   * [Command Line Tool](#command-line-tool)
      * [`update`](#update) - update biodiversity dataset graph
      * [`ls`](#ls) - list/print biodiversity dataset graph
      * [`get`](#get) - print biodiversity dataset graph node (e.g., dwca)
      * [`history`](#history) - show history of biodiversity dataset graph node
   * [Use Cases](#use-cases)
      * [`mining citations`](#mining-citations)
      * [`archiving`](#archiving)
      * [`web access`](#web-access)
      * [`data access monitor`](#data-access-monitor)
      * [`compare versions`](#compare-versions)
      * [`generating citations`](#generating-citations)
      * [`finding copies using hash-archive.org`](#finding-copies-using-hash-archiveorg)
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

The ```update``` command updates your local biodiversity dataset graph using remote resources. By default, Preston uses GBIF, iDigBio and BioCASe to retrieve associated registries and data archives. The output is statements, expressed in nquads (or nquad-like tsv). An in depth discussion of rdf, nquads and related topics are beyond the current scope. However, with a little patience, you can probably figure out what Preston is trying to communicate. 

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

If you don't want to download the entire biodiversity dataset graph (~60GB) onto your computer, you can also use [GBIF's dataset registry search api](https://www.gbif.org/developer/registry) as a starting point. For instance, if you run ```preston update "http://api.gbif.org/v1/dataset/suggest?q=Amazon&amp;type=OCCURRENCE"```, you only get occurence datasets that GBIF suggests are related to the Amazon. If you track these suggested datasets, you might see something like:

```console
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/dc/elements/1.1/format> "application/dwca" .
<hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-05T05:11:33.592Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
<hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> <http://www.w3.org/ns/prov#wasGeneratedBy> <21de25a8-927f-49a1-99be-725f1f506232> .
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/pav/hasVersion> <hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> .
```

which tells us that a [darwin core archive](http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip) was found and a copy of it was made on 2018-09-05. The copy, or version, has a content hash of hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8 . Incidentally, you can reach this same exact dataset at [web-accessible preston archive](https://deeplinker.bio/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8). With this, we established that on 2018-09-05 a specific web addressed produced a specific content. On the next update run, Preston will download the content again. If the content is the same as before, nothing happens. If the content changed, a new version will be created associated with the same address, establishing a versioning of the content produced by the web address. This is addressed in a statement like ```<some hash> <.../previousVersion> <some previous hash>```. 

So, in a nutshell, the update process produces a detailed record of which resources are downloaded, what they look like and were they came from. You can retrieve the record of a successful run by using `ls`.

#### `ls`  

`ls` print the results of the previous updates. An update always refers to a previous update, so that a complete history can be printed / replayed of all past updates. So, the `ls` commands lists your (local) copy of the biodiversity dataset graph. 

#### `get`

`get` retrieves a specific node in the biodiversity dataset graph. This can be a darwin core archive, EML file but also a copy of the iDigBio publisher registry. For instance, if you'd like to retrieve the node with DwC-A content, get the file and list the content using ```unzip``` and access the references in the taxa.txt file.

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
preston history
<0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .
<hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-04T20:48:35.096Z" .
<hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> <http://purl.org/pav/previousVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .
<hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93> <http://www.w3.org/ns/prov#generatedAtTime> "2018-09-04T23:14:22.292Z" .
<hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93> <http://purl.org/pav/previousVersion> <hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> .
...
``` 

By default, the `history` command shows the versions of your local biodiversity dataset graph as a **whole**. A list of versions associated with the sequence of updates. If you'd like to know what the UUID 0659a54f-b713-4f86-a917-5be166a14110 is described as, you can use `ls` and filter by the UUID:

```console
$ preston ls | grep 0659a54f-b713-4f86-a917-5be166a14110
<0659a54f-b713-4f86-a917-5be166a14110> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Entity> .
<0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/dc/terms/description> "A biodiversity dataset graph archive."@en .
```

So, the UUID ending on 4110 is describe as "A biodiversity dataset graph archive". This UUID is the same across all Preston updates, so in a way we are help to create different versions of the same "a biodiversity dataset graph". Good to know right? 

You can also use `history` for a specific url, like:

```console
$ preston history http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip
<http://plazi.cs.umb.edu/GgServer/dwca/FFBEFF81FE1A9007FFDFFC38FFDCFF90.zip> <http://purl.org/pav/hasVersion> <hash://sha256/5cba2f513fee9e1811fe023d54e074df2d562b4169b801f15abacd772e7528f8> .
```

### Use Cases

In the previous section the commands `update`, `ls`, `get` and `history` were introduced. Now, some use cases are covered to show how to combine these basic commands to make for useful operations. This is by no means an exhaustive list of all the potential uses, but instead is just to provide some inspiration on how to get the most out of preston.

#### Mining Citations

The Ecological Metadata Language (EML) files contain citations, and your biodiversity dataset graph contains EML files. To extract all citations you can do:

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

For instance, assuming that a preston data directory exists on a ```serverA``` which has ssh and rsync installed on it, you can keep a local copy in sync by running:

```
$ rsync -Pavz preston@someserver:~/preston-archive/data /home/someuser/preston-archive/
```

On a consumer internet connection with bandwidth < 10Mb/s, an initial sync with a remote trans-atlantic server with a 67GB preston archive took about 3 days. After the initial sync, only files that you don't have yet are included. For instance, if no new files are added to the remote preston archive, a sync take a few minutes instead of hours or days. 

Note that ssh and rsync comes with frequently used linux distributions like Ubuntu v18.04 by default). 

#### Web Access

If you'd like to make your Preston archive accessible via http/https by using a [nginx webserver](http://nginx.org), you can use a following address mapping to your [nginx configuration](https://nginx.org/en/docs/beginners_guide.html):

```console
location ~ "/\.well-known/genid/" {
		return 302 https://www.w3.org/TR/rdf11-concepts/#section-skolemization;
}

location ~ "^/([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{58})$" {
            	try_files /preston/$1/$2/$3/$uri/data =404;
}
```
The first ```location``` block redirects any URIs describing [skolemized blank nodes](https://www.w3.org/TR/rdf11-concepts/#section-skolemization) to the appropriate [w3c](https://w3c.org) documentation on the topic. The second ```location``` block configures the server to attempt to retrieve a static file with a 64 hexadecimal sha256 hash from the appropriate ```data``` file in preston archive directory on the web server. 

With this, you can access your Preston archive remotely using the URLs like ```https://someserver/[sha256 content hash]``` . So, if you'd like to dereference (or download) ```hash://sha256/1846abf2b9623697cf9b2212e019bc1f6dc4a20da51b3b5629bfb964dc808c02``` , you can now point your http client or browser at ```https://someserver/1846abf2b9623697cf9b2212e019bc1f6dc4a20da51b3b5629bfb964dc808c02``` . Note that you do not need any other software than the (standard) nginx webserver, because you are serving the content as static files from the file system of your server. 

#### Data Access Monitor

By running [`update`](#update) periodically and checking for "blank", or "missing" nodes (see [blank skolemization](https://www.w3.org/TR/rdf11-concepts/#section-skolemization)), you can make a list of the dataset providers that went offline or are not responding.

The example below was created on 2018-09-05 using biodiversity dataset graph with hash [hash://sha256/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93](https://deeplinker.bio/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93).

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

Preston provides both a date and a content-based identifier for the datasets that you are using and the biodiversity dataset graph as a whole. Also, it produces the information in a format that is machine readable. This enables the automated generation of citations, for human or machine consumption, as evidenced by the reference to a [particular version of the biodiversity dataset graph](https://deeplinker.bio/7efdea9263e57605d2d2d8b79ccd26a55743123d0c974140c72c8c1cfc679b93) in the previous section. 

So now, instead of a citation to a dataset like:

Levatich T, Padilla F (2017). EOD - eBird Observation Dataset. Cornell Lab of Ornithology. Occurrence dataset [https://doi.org/10.15468/aomfnb](https://doi.org/10.15468/aomfnb) accessed via [GBIF.org](https://gbif.org) on 2018-09-11.

A citation might look something like:

Levatich T, Padilla F (2017). EOD - eBird Observation Dataset. Cornell Lab of Ornithology. Occurrence dataset [hash://sha256/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c](https://deeplinker.bio/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c) accessed at [http://ebirddata.ornith.cornell.edu/downloads/gbiff/dwca-1.0.zip](http://ebirddata.ornith.cornell.edu/downloads/gbiff/dwca-1.0.zip) at 2018-09-02 with provenance [hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d](https://deeplinker.bio/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d) .

The latter citation tells you exactly what file was used and where it came from. The former tells you that some eBird dataset was accessed via GBIF on a specific date and leaves it up to the reader to figure out exactly which dataset was used.

#### Finding copies with hash-archive.org

[hash-archive.org](https://hash-archive.org) is a project by [Ben Trask](https://bentrask.com), the same person who suggested to use hash uris to represent content hashes (e.g., hash://sha256/...). The hash archive keeps track of what content specific urls created using content hashes. To make the hash archive update the hash associated with a url, you can send a http get request in the form of ```https://hash-archive.org/api/enqueue/[some url]``` . For example, to register a url that is known to host an DwC-A at ```http://zoobank.org:8080/ipt/eml.do?r=zoobank```, you can click on https://hash-archive.org/api/enqueue/https://hash-archive.org/history/http://zoobank.org:8080/ipt/eml.do?r=zoobank , or using curl like 


```sh
curl https://hash-archive.org/api/enqueue/http://zoobank.org:8080/ipt/eml.do?r=zoobank
```

On successful completion of the request, hash-archive.org returns something like:

```json
{
    "url": "http://zoobank.org:8080/ipt/eml.do?r=zoobank",
    "timestamp": 1537269631,
    "status": 200,
    "type": "text/xml;charset=utf-8",
    "length": 3905,
    "hashes": [
        "md5-zyn7V5JlXkrqxJILT8ZfGw==",
        "sha1-SALPtju8vNii2S/Rt3R946iKc0g=",
        "sha256-yBrrBSjo86D8U4mniRsigr4ijoTAtXZ2aSJlhcTa1sQ=",
        "sha384-IguP+tlrYZ8QVBC86YIPxf/7CWFhU2HTzxI2DYLq40mo1dwcS5yn6qJb0SatWaUH",
        "sha512-sl3/Qm7Jd965F+QLkxbp/Xdsv7ZwWX6HKDgpwXk3OLyOGWpgym1HBSOEhRtMiH2g7MZzwKjyEyL4PajQAinj"
    ]
}
```

This response indicates that the hash archive has independently downloaded the EML url, and calculated various content hashes. Now, you should be able to do to https://hash-archive.org/history/http://zoobank.org:8080/ipt/eml.do?r=zoobank , and see the history of content that this particular url has produced. 

In short, hash-archive provides a way to check whether content produced by a url has changed. Also, it provides a way to lookup which urls are associated with a unique content hash. 

The example (also see related [gist](https://gist.github.com/jhpoelen/0f531a8489c1001e92aae4c94a003ba3)) below shows how Preston was used to register biodiversity source urls as well as Preston web-accessible urls via https://deeplinker.bio (see [Web Access](#web-access)). 

```bash
#!/bin/bash
# Register all preston urls with hash-archive.org
#
# Please replace "deeplinker\.bio" instances below with you own escaped hostname of your Preston instance.

# see https://preston.guoda.bio on how to install preston
#

preston ls -l tsv | grep Version | cut -f1,3 | tr '\t' '\n' | grep -v "deeplinker\.bio/\.well-known/genid" | sort | uniq | sed -e 's/hash:\/\/sha256/https:\/\/deeplinker.bio/g' | sed -e 's/^/https:\/\/hash-archive.org\/api\/enqueue\//g' | xargs -L1 curl 
```

If all web-accessible Preston instances would periodically register their content like this, https://hash-archive.org could serve as a way to lookup backup for the an archive that you got from some no longer active archive url.


## Prerequisites

Preston needs Java 8+.

## Install

Preston is a stand-alone java application, packaged in a jarfile. You can build you own (see [building](#building)) or download a prebuilt jar at [releases](https://github.com/bio-guoda/preston/releases).

On linux (and Mac) you can install Preston by running:

```console
sudo sh -c '(echo "#!/usr/bin/env sh" && curl -L https://github.com/bio-guoda/preston/releases/download/0.0.5/preston.jar) > /usr/local/bin/preston && chmod +x /usr/local/bin/preston' && preston version
```

On successful installation, execute ```preston version``` on the commandline should print the version of preston. 
.
Alternatively, you can download the jar manually and run preston by using commands like ```java -jar preston.jar version```.

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
    ls      list biodiversity dataset graph
      Usage: ls [options]
        Options:
          -l, --log
            log format
            Default: nquads
            Possible Values: [tsv, nquads, dots]

    get      get biodiversity node(s)
      Usage: get node id (e.g., [hash://sha256/8ed311...])

    update      update biodiversity dataset graph
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
