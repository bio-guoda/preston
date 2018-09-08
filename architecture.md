# Preston Architecture

Preston consist of the following components: a [`crawler`](#crawler), [`content handlers`](#content-handlers), an [`archiver`](#archiver), a content addressed [`blob store`](#blob-store), and a [`simplified hexastore`](#simplified-hexastore). In the following sections, each component is discussed.  

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

An archiver listens to statements containing  a _blank_ . On receiving such a statement, the archiver attempts to dereference the resources (e.g., https://search.idigbio.org/v2/search/publishers) by attempting to download the content associated to the resource. On successfully downloading the content, the content is put into the [`blob store`](#blob-store) and the relationship between the publisher, the version term and the content is stored in the [`simple hexastore`](#statement-store) as a key value pair. More on that later.

## `blob store`

On succesfully saving the content into the blob store, a unique identifier is returned in the form of a SHA256 hash. The unique content identifier is now used to store a relation between the resource and it's unique content identifier. This identifier is now used to point to the content. Also, the content is saved in an hierarchical file structure derived from the content hash. For example, if the url https://search.idigbio.org/v2/search/publishers resolved to content with a hash of hash://sha256/3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef (see https://hash-store.org for the hash url notation), then a file called "data" is stored in the following structure:

```
3e/
    df/
        e3/
            3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef/
                data
```

With the file path being derived from the hash of the data itself, you can now easily locate the content by its hash. For instance, on the server at https://deeplinker.io , the nginx webserver is configured such that you can retrieve the said datafile by requesting https://deeplinker.io/3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef . Note that this content hash is "real" and you can download the copy (or version) of the content that was served by https://search.idigbio.org/v2/search/publishers at some point in the past. So, using the blob store, we know have a way to easily access content as long as we know the content hash.  

## `simple hexastore`

The simple hexastore contains relationships that connect resources with their content using predicates (or verbs). The relationship is stored by combining a hashed "hasVersion" relationship (or predicate) with the hashed resource url. This combination is now turned into a unique identifier also, by adding the two hash urls and hashing the result. For example:

```
sha256(
  sha256(<https://search.idigbio.org/v2/search/publishers>) -> hash://sha256/3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef
+
  sha256(<http://purl.org/pav/hasVersion>) -> hash://sha256/0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806
) = hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e 
```

Note that on a *nix command line, you can calculate the hashes using the `sha256sum` program like:
```console
$ echo -n "https://search.idigbio.org/v2/search/publishers" | sha256sum
3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef  -
```

So, lets say that the archiver has dereferenced the publisher url to content with the hash identifier hash://sha256/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 . Now the archiver stores the publisher/hasVersion hash as a key with value hash://sha256/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 into the statement (or relationstore) store. With this, we can retrieve the first version of the deferenced publisher content by lookup up the content of the hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e . This effectively implements a simplified version of a hexastore in which queries (e.g., what is the content hash of the content retrieve from https://search.idigbio.org/v2/search/publisher ? ) can be answered by dereferencing (or downloading) the content of the combined hash key of publisher url and hasVersion term. You can do this now using https://deeplinker.bio/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e .

The simple hexastore itself uses the same folder structure as the blob store to store the value associated with the hash key like:

```
a2/
    1d/
        81/
            a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e/
                data
```

As you might have seen, deeplinker.bio, resolves https://deeplinker.bio/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e to hash://sha256/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 . The latter hash can now be used to resolves to the specific version of https://search.idigbio.org/v2/search/publishers . Using curl, jq, and head, the first the lines of the json content can be shown: 

```console
$ curl --silent https://deeplinker.bio/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 | jq . | head -n10
{
  "itemCount": 78,
  "items": [
    {
      "uuid": "51290816-f682-4e38-a06c-03bf5df2442d",
      "type": "publishers",
      "etag": "8042902af0c83d7de00d42711bd3e4420c44452a",
      "data": {
        "rss_url": "https://www.morphosource.org/rss/ms.rss",
        "name": "MorphoSource RSS feed",
...
```

Perhaps unsurprisingly, the sha256 hash of this content is the same is before:

```
$ curl --silent https://deeplinker.bio/3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362 | sha256sum
3eff98d4b66368fd8d1f8fa1af6a057774d8a407a4771490beeb9e7add76f362  -
```

## summary 

In summary, Preson combines a crawler, registry content handlers, and an archiver with a blob store and simple hexastore to implement a relatively simple scheme to record a versioned provenance and the content of datasets and their registries. By using a hashes to uniquely identify both dereferenced (or downloaded) content and simply queries (what content was downloaded from a specific url?) a simple file structure can be used to serve content and answer queries. Because the hashing schemes are applied consistently, each and every preston based blob and hexastore can be used to reliably retrieve content as well as query provenance of that content.


## examples

The example below show some example of how to query a Preston file structure or web accessible store for versions of things. 

description | resource (subject) | relationship (predicate) | search key (subject+predicate) | content hash (object)
 --- | --- | --- | --- | ---
a version of eBird's dwca | http://ebirddata.ornith.cornell.edu/downloads/gbiff/dwca-1.0.zip | http://purl.org/pav/hasVersion | hash://sha256/ae27e5a9612ab3754f8160922abf6c5c6ffc6b5a077f3e684d1ce57605929eb6 | hash://sha256/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c (resolved via [deeplinker.io](https://deeplinker.io/ae27e5a9612ab3754f8160922abf6c5c6ffc6b5a077f3e684d1ce57605929eb6)) 
a version of biodiversity data graph in nquads | 0659a54f-b713-4f86-a917-5be166a14110 (uuid of the biodiversity dataset graph idea) | http://purl.org/pav/hasVersion | hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a | hash://sha256/c253a5311a20c2fc082bf9bac87a1ec5eb6e4e51ff936e7be20c29c8e77dee55 (resolved via [deeplinker.io](https://deeplinker.io/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a)).
