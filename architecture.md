# Preston Architecture

Preston consist of the following components: a [`crawler`](#crawler), [`content handlers`](#content-handlers), an [`archiver`](#archiver), a content addressed [`blob store`](#blob-store), and a [`simplified hexastore`](#simplified-hexastore). In the following sections, each component is discussed, ending with a [`summary`](#summary). 

Please [open an issue](https://github.com/bio-guoda/preston) if you have any questions, comments and suggestions.

## ```crawler```

The crawler creates statements that describe the crawling activity. An example of one of the many crawler statements is the start time of the crawl, or, as expressed in nquad speak:

```
<e8a41d42-3688-43a7-b287-b78b8d485a2c> <http://purl.org/dc/terms/description> "A crawl event that discovers biodiversity archives."@en .
<e8a41d42-3688-43a7-b287-b78b8d485a2c> <http://www.w3.org/ns/prov#startedAtTime> "2018-09-07T12:43:10.322Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
```


The first line defined the crawl event or activity, and the second line states that this particular crawl event was started at some time on 2018-09-07.

The crawler also issues statements that describe registries of biodiversity datasets, or more specifically:

```
<https://idigbio.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Organization> .
<https://search.idigbio.org/v2/search/publishers> <http://purl.org/dc/terms/description> "Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors."@en .
<https://search.idigbio.org/v2/search/publishers> <http://purl.org/pav/createdBy> <https://idigbio.org> .
<https://search.idigbio.org/v2/search/publishers>
<https://search.idigbio.org/v2/search/publishers> <http://purl.org/dc/elements/1.1/format> "application/json" .
```

The statement above describe idigbio as an organization that created a resource, https://search.idigbio.org/v2/search/publishers, which has a format "application/json". 

On finishing a crawl, the complete record of the crawl is stored into the ```blobstore```. Also, the [`simplified hexastore`](#simplified-hexastore) is used to descibe the stored record as a version of the biodiversity dataset graph. Note also, that at the start of a successive crawl, a relation with the previous crawl is created. This relation helps to retain the provenance and versions of the various versions of the biodiversity dataset graph.  

So, in short, the crawler produces, stores and versions the information that describes a crawl, including registries like the list of publishers created by iDigBio and retrieved archives. The versions of the crawl can be interpreted as versions of a biodiversity dataset graph.

## `content handlers`

Content handlers listen to statements created by the crawler by other content handlers. In addition, content handlers can create (or _emit_) statement of their own. 

For instance, the iDigBio registry content handler responds to statement that include the iDigBio publisher registry. On receiving such as statement, the handler requests a version of the registry creating (or _emitting_) a question, or statement with a _blank_. Such a question looks something like:

```
<https://search.idigbio.org/v2/search/publishers> <http://purl.org/pav/hasVersion> _:5bd91b46-dffb-36f2-9547-7acc61f50117 . 
``` 
This statements says that the iDigBio publisher registry has version _blank_ . This is the way for a content handler to ask the [`archiver`](#archiver) to fill in the _blank_ by attempting to dereference (e.g., download, "click on", or lookup) the content associated with https://search.idigbio.org/v2/search/publishers . 

If the content handler received a statement with a non-blank resource version, like:

```
<https://search.idigbio.org/v2/search/publishers> <http://purl.org/pav/hasVersion> <hash://sha256/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362> .
```

then the content handler will attempt to parse the content. In parsing the content, more statements can be created, including requests for, let's say downloading of darwin core archives or rss feeds using a ```<resource> <hasVersion> <blank> . ``` as we saw earlier.

## `archiver`

An archiver listens to statements containing  a _blank_ . On receiving such a statement, the archiver attempts to dereference the resources (e.g., https://search.idigbio.org/v2/search/publishers) by attempting to download the content associated to the resource. On successfully downloading the content, the content is put into the [`blob store`](#blob-store) and the relationship between the publisher, the version term and the content is stored in the [`simplified hexastore`](#simplified-hexastore) as a key value pair. More on that later.

## `blob store`

On succesfully saving the content into the blob store, a unique identifier is returned in the form of a SHA256 hash. The unique content identifier is now used to store a relation between the resource and it's unique content identifier. This identifier is now used to point to the content. Also, the content is saved in an hierarchical file structure derived from the content hash. For example, if the url https://search.idigbio.org/v2/search/publishers resolved to content with a hash of hash://sha256/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 (see [the "official" spec of hash uri notation](https://github.com/hash-uri/hash-uri/blob/master/README.md) with examples at [hash-archive.org](https://hash-archive.org)), then a file is stored in the following structure:

```
3e/
    ff/
        3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362
```

With the file path being derived from the hash of the data itself, you can now easily locate the content by its hash. For instance, on the server at https://deeplinker.bio , the nginx webserver is configured such that you can retrieve the said datafile by requesting https://deeplinker.bio/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 . Note that this content hash is "real" and you can download the copy (or version) of the content that was served by https://search.idigbio.org/v2/search/publishers at some point in the past. So, using the blob store, we know have a way to easily access content as long as we know the content hash.

## `simplified hexastore`

The simplified hexastore ([Weiss et al. 2008](#weiss2008)) keeps an index of the immutable versions of the biodiversity dataset graph provenance over time. This index allows to traverse from the first version of the provenance to the next. The index is created by hashing a query (what is the next version?) and associating this query hash to the content hash uri of the related provenance log version. 

The query hash is calculated by combining the hashed resource uri with a hashed version relationship (e.g., ```hasVersion``` or ```previousVersion```). These two hash uri are appended and the result is hashed again to create a unique query hash uri. For example, the query for the first version of a biodiversity dataset graph is always:

```
sha256(
  sha256(<0659a54f-b713-4f86-a917-5be166a14110>)
  + sha256(<http://purl.org/pav/hasVersion>)
) -->

sha256(
  hash://sha256/1a9158fc90d1b38fe7fa71118daa88861c0d40761e4c1452c64e069c35617271
  + hash://sha256/0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806
) -->

hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e 
```

where ```<0659a54f-b713-4f86-a917-5be166a14110>``` uniquely identifies the (abstract) concept of a biodiversity dataset graph. This is a hardcoded value that is referenced in all Preston provenance logs.

On a Linux command line, you can calculate the hashes using the `sha256sum` program like:

```console
$ echo -n "0659a54f-b713-4f86-a917-5be166a14110" | sha256sum
1a9158fc90d1b38fe7fa71118daa88861c0d40761e4c1452c64e069c35617271  -
```

and

```console
$ echo -n "http://purl.org/pav/hasVersion" | sha256sum
0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806  -
```

and computing the query hash by combining the hashes:

```console
$ echo -n "hash://sha256/1a9158fc90d1b38fe7fa71118daa88861c0d40761e4c1452c64e069c35617271hash://sha256/0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806" | sha256sum
2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a  -
```

On saving a Preston provenance log for the first time, Preston puts the content hash uri of that provenance log (e.g., ```hash://sha256/c253...```) in a file named by the query hash ```hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55``` into the statement (or relation,version) store. With this, on requesting content associated with ```hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a```, the content hash uri of the first provenance log (i.e., ```hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55```) is returned. The content of the first version can now be retrieved via the content store. Because the first query is static, the content hash uri of the first provenance log version of any Preston observatory can be retrieved by resolving ```hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e``` . In order words, if a hash ```a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e``` is found anywhere in a data archive, the chances are pretty high that the archive is a contains Preston provenance logs.

The next version in the provenance history can be retrieved using:

```
sha256(
  sha256(<http://purl.org/pav/previousVersion>)
  + sha256(<hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55>)
  + 
) -->

sha256(
  hash://sha256/718cc4ed3f9f39852e185e8712d775ac95d798ac7795c4adc98e4b73fd4528b8
  + hash://sha256/130ae3d05860975b540e6572b3a5d7bb8fbd7e1b7d62416ae654354b59e87c77
) -->

hash://sha256/7ebb008412baaac3afcc8af68b796bf4ca98f367cfd61a815eee82cdffeab196 
```

Continuing this pattern, all available provenance log versions can be traversed until a query result is empty or generates a 404 not found error. This simplified hexastore enables the ```history``` command:

```
$ preston history --remote https://deeplinker.bio | head -n2
<0659a54f-b713-4f86-a917-5be166a14110> <http://purl.org/pav/hasVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .
<hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d> <http://purl.org/pav/previousVersion> <hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> .
```

The simplified hexastore uses the same folder structure as the blob store to store the value associated with the hash key like:

```
2a/
    5d/
        2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a
```

As you might have seen, deeplinker.bio, resolves ```https://deeplinker.bio/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a``` to a response with content ```hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55``` . The latter hash can now be used to resolves to the first version of the provenance log version history . Using curl, head, the first the lines of the first provenance log content can be shown: 

```console
$ curl --silent https://deeplinker.bio/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55 \
    | head
<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .
<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Agent> .
<https://preston.guoda.org> <http://purl.org/dc/terms/description> "Preston is a software program that finds, archives and provides access to biodiversity datasets."@en .
<1d711945-d205-4663-b534-6d706b8b77b6> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Activity> .
<1d711945-d205-4663-b534-6d706b8b77b6> <http://purl.org/dc/terms/description> "A crawl event that discovers biodiversity archives."@en .
<1d711945-d205-4663-b534-6d706b8b77b6> <http://www.w3.org/ns/prov#startedAtTime> "2018-09-04T07:29:11.130Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> .
...
```

Perhaps unsurprisingly, the sha256 content hash of the provenance log version remains the same:

```
$ curl --silent https://deeplinker.bio/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55 | sha256sum
c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55  -
```

The simplified hexastore is intended as a pragmatic way to discover a history of provenance logs. The integrity of this history can be verified because every successive provenance log version references the preceeding one using a statement like:

```
<hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55> <http://www.w3.org/ns/prov#usedBy> <4e540f45-d7a1-40d6-a2b8-f623f1c1d566> .
```

where ```4e540f45-d7a1-40d6-a2b8-f623f1c1d566``` uniquely identifies a specific crawl activity. This statement can be found on line 11 in provenance log identified by [hash://sha256/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d](https://deeplinker.bio/b83cf099449dae3f633af618b19d05013953e7a1d7d97bc5ac01afd7bd9abe5d) .

For more information about hexastores, please see reference [Weiss et al. 2008](#weiss2008) .

## summary 

[Preston](https://github.com/bio-guoda/preston) combines a [`crawler`](#crawler), [`content handlers`](#content-handlers), and an [`archiver`](#archiver) with a [`blob store`](#blob-store) and [`simplified hexastore`](#simplified-hexastore) to implement a scheme to establish an immutable, versioned, provenance of a biodiversity dataset graph over time. By using a hashes to uniquely identify both dereferenced (or downloaded) content and simply queries (what content was downloaded from a specific url?) a simple file structure can be used to serve content and answer queries. Because the hashing schemes are applied consistently, each and every preston based blob and hexastore can be used to reliably retrieve content as well as query provenance of that content.


## examples

The example below show some example of how to query for versions of things in a Preston file structure or web accessible store.

### eBird
eBird is the biggest collection of occurrence data that I know of. ~10GB.

description | a version of eBird's dwca 
 --- | --- 
resource (subject)	| http://ebirddata.ornith.cornell.edu/downloads/gbiff/dwca-1.0.zip	
relationship (predicate) |	http://purl.org/pav/hasVersion
search key (subject+predicate) |	hash://sha256/ae27e5a9612ab3754f8160922abf6c5c6ffc6b5a077f3e684d1ce57605929eb6	
content hash (object)|	hash://sha256/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c (resolved via [deeplinker.bio](https://deeplinker.bio/ae27e5a9612ab3754f8160922abf6c5c6ffc6b5a077f3e684d1ce57605929eb6))

### a first biodiversity dataset graph version
The record of a crawl session can also be interpreted as a version of a biodiversity graph. Here's how you retrieve the first version:

description | a version of biodiversity dataset graph 
 --- | --- 
resource (subject)  | 0659a54f-b713-4f86-a917-5be166a14110 (uuid of the biodiversity dataset graph)
relationship (predicate) |  http://purl.org/pav/hasVersion
search key (subject+predicate)	 | hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a
content hash (object)	| hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55 (resolved via [deeplinker.bio](https://deeplinker.bio/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a)).

## References

### Weiss2008
Weiss, C., Karras, P. and Bernstein, A., 2008. Hexastore: sextuple indexing for semantic web data management. Proceedings of the VLDB Endowment, 1(1), pp.1008-1019. Also see [https://people.csail.mit.edu/tdanford/6830papers/weiss-hexastore.pdf](https://people.csail.mit.edu/tdanford/6830papers/weiss-hexastore.pdf) .


