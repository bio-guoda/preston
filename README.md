# Preston*: a biodiversity dataset tracker

[![Build Status](https://travis-ci.org/bio-guoda/preston.svg?branch=master)](https://travis-ci.org/bio-guoda/preston) [![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme) 

 *Named after [Frank W. Preston (1896-1989)](https://en.wikipedia.org/wiki/Frank_W._Preston) and the Prestonian shortfall, one of the "[Seven Shortfalls that Beset Large-Scale Knowledge of Biodiversity](https://doi.org/10.1146/annurev-ecolsys-112414-054400)" as described by [Hortal et al. 2015](https://doi.org/10.1146/annurev-ecolsys-112414-054400).  

## Table of Contents

- [Prerequisites](#prerequisites)
- [Install](#install)
- [Usage](#usage)
- [Examples](#examples)
- [Building](#building)
- [Contribute](#contribute)
- [License](#license)

## Prerequisites

Preston needs Java 8+.

## Install

Preston is a stand-alone java application, packaged in a jarfile. You can build you own (see [building](#building)) or download a prebuilt jar at [releases](https://github.com/bio-guoda/preston/releases).

On linux (and Mac) it is recommended to make an alias by appending the following to ~/.bash_aliases :

```
alias nomer='java -Xmx4G -jar [some dir]/preston.jar'
```

where [some dir] is the location where preston.jar lives. With this alias, you can now do ```preston version``` instead of ```java -jar preston.jar version```.

### Maven, Gradle, SBT
Nomer is made available through a [maven](https://maven.apache.org) repository.

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

### Building

Please use [maven](https://maven.apache.org) version 3.3+.

* Clone this repository
* Run tests using `mvn test` (optional).
* Run `mvn package -DskipTests` to build (standalone) jar
* Copy `preston/target/preston-[version]-jar-with-dependencies.jar` to ```[some dir]/preston.jar```

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

## Examples 

## Contribute
Feel free to join in. All welcome. Open an [issue](https://github.com/bio-guoda/preston/issues)!

## License

[MIT](LICENSE)
