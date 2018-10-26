# Preston Analysis

Preston records how and when datasets are discovered and accessed in the rdf/nquads. These crawl records, or biodiversity dataset graphs, can be loaded into triple stores like [Apache Jena Fuseki](https://jena.apache.org/documentation/fuseki2/) for discovery and analysis. 

This page contains some [sparql](https://www.w3.org/TR/rdf-sparql-query/) queries to discover and analyze the dataset graph. 

# Use Cases

## Detecting Linkrot

[Linkrot](https://en.wikipedia.org/wiki/Link_rot), a well documented, and often occurring, phenomenon in which content associated to links become permanently unavailable.  

URLs are used to access content. While the URLs might be static, the content is often not. High turnover or error rates in content linked to by a url can be a sign of an instable, actively maintained, or randomly changing datasets. Since Preston is continuously tracking urls and their content, we can use its output, a biodiversity dataset graph, to detect linkrot. 

### Change Rate of Urls

To detect the rate of change of urls the following query was created. This query generates a list of urls in decreasing order of change rate. Note that Preston records each failed attempt to access a url's content as "blank" content. 

```sparql
SELECT DISTINCT ?url (COUNT(?url) as ?totalVersions)
WHERE {
  { ?url <http://purl.org/pav/hasVersion> ?firstVersion }
  { ?aVersion (<http://purl.org/pav/previousVersion>|^<http://purl.org/pav/previousVersion>)* ?firstVersion }
  { ?aVersion <http://www.w3.org/ns/prov#generatedAtTime> ?generationTime }
} GROUP BY ?url ORDER BY DESC(?totalVersions)
LIMIT 10
```

Using Poelen, Jorrit H. (2018). A biodiversity dataset graph (Version 0.0.1) [Data set]. Zenodo. http://doi.org/10.5281/zenodo.1472394 , this created the following results:

url |	totalVersions
--- | ---
http://bim-mirror.aseanbiodiversity.org:8080/ipt/archive.do?r=flora_of_agusanmarsh | 140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/archive.do?r=iccemclsu-rsa_pantabangan_fauna | 140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/archive.do?r=rgonzalez_museum |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=camiguin_fauna |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=fauna_for_borneo |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=flora_of_agusanmarsh |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=iccemclsu-rsa_pantabangan_fauna |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=ipaskitanglad |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=mcme_uplb_museum_plant_collection |	140
http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=mmfrph_zingi |	140


### Tracking the Origin of a URL

Now that we noticed that some urls have many version, we'd like to understand how the url was discovered.

```sparql
SELECT ?originUrl ?originHash ?originCollection ?dateTime
WHERE {
  { ?originCollection <http://www.w3.org/ns/prov#hadMember> <http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=mmfrph_zingi> }
  { ?originHash ?p ?originCollection }
  { ?originHash <http://www.w3.org/ns/prov#generatedAtTime> ?dateTime }
  { ?originUrl <http://purl.org/pav/hasVersion> ?x }
  { ?x (<http://purl.org/pav/hasVersion>|^<http://purl.org/pav/previousVersion>)* ?originHash }
}
LIMIT 10
```

which produced the following result:

originUrl   | originHash | originCollection | dateTime
---   | --- | --- | ---
https://api.gbif.org/v1/dataset?offset=13480&limit=20 | hash://sha256/e285cbc69418e2847a3727ec650edfc7e1c405dc71dc9a93b859a28028e79cab | af32ab2e-7be6-42ca-a570-ad79fe0e32bb | 2018-09-01T19:02:16.675Z


Now, we know that the result was retrieved from the gbif registry via https://api.gbif.org/v1/dataset?offset=13480&limit=20 on 2018-09-01T19:02:16.675Z with content hash [hash://sha256/e285cbc69418e2847a3727ec650edfc7e1c405dc71dc9a93b859a28028e79cab](https://deeplinker.bio/e285cbc69418e2847a3727ec650edfc7e1c405dc71dc9a93b859a28028e79cab). After retrieving that specific registry chunk, we notice: 

```json
{
  ...
      "key": "af32ab2e-7be6-42ca-a570-ad79fe0e32bb",
      "installationKey": "286d31fd-2be5-4df7-be2d-20448e158c81",
      "publishingOrganizationKey": "a30d7f59-d3d4-4e89-97dc-de9cf837f591",
      "doi": "10.15468/b95s7t",
     ...
     "endpoints": [
        {
          "key": 260345,
          "type": "DWC_ARCHIVE",
          "url": "https://orphans.gbif.org/af2a0fa1-4c8e-4bdc-8954-b1a55e32b0f1/af32ab2e-7be6-42ca-a570-ad79fe0e32bb.zip",
          "description": "Orphaned dataset awaiting adoption.",
          "createdBy": "MattBlissett",
          "modifiedBy": "MattBlissett",
          "created": "2018-03-06T16:14:55.983+0000",
          "modified": "2018-03-06T16:14:55.983+0000",
          "machineTags": []
        },
        {
          "key": 127629,
          "type": "EML",
          "url": "http://bim-mirror.aseanbiodiversity.org:8080/ipt/eml.do?r=mmfrph_zingi",
          "createdBy": "a30d7f59-d3d4-4e89-97dc-de9cf837f591",
          "modifiedBy": "a30d7f59-d3d4-4e89-97dc-de9cf837f591",
          "created": "2016-07-13T05:53:31.072+0000",
          "modified": "2016-07-13T05:53:31.072+0000",
          "machineTags": []
        }
      ],
   ...
}
```

Which seem to indicate tha the DWC-a of this dataset was orphaned and (temporarily?) archived by GBIF. However, our suspicious url, the EML file, was not orphaned or (temporarily?) archived. 

On inspecting different versions of the EML file, we find most versions are blank nodes, indicating a failed attempt to retrieve the content. In one instance some content was retrieved: [hash://sha256/9944f274ee46c33a577e170bb3fd85a4b824741eb7bcc18a002c8b77ca8f3e3a](https://deeplinker.bio/9944f274ee46c33a577e170bb3fd85a4b824741eb7bcc18a002c8b77ca8f3e3a). This specific content turns out to be some html page, not an advertised EML file. The first few lines of this html page with hash ending on 3e3a looks like:

```html
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
    <head>
 	    <meta name="copyright" lang="en" content="GBIF" />
 		<title>IPT setup</title>
	  <link rel="stylesheet" type="text/css" media="all" href="http://bim-mirror.aseanbiodiversity.org:8080/ipt/styles/reset.css" />
		<link rel="stylesheet" type="text/css" media="all" href="http://bim-mirror.aseanbiodiversity.org:8080/ipt/styles/text.css" />
		<link rel="stylesheet" type="text/css" media="all" href="http://bim-mirror.aseanbiodiversity.org:8080/ipt/styles/960_24_col.css" />
 		<link rel="stylesheet" type="text/css" href="http://bim-mirror.aseanbiodiversity.org:8080/ipt/styles/main.css"/>
 		<link rel="shortcut icon" href="http://bim-mirror.aseanbiodiversity.org:8080/ipt/images/icons/favicon-16x16.png" type="image/x-icon" />
                ...
```

### Conclusion
We were able to detect ongoing outages (or bitrot) of an EML file related to a dataset that is registered in the GBIF network using a biodiversity dataset graph tracked by a Preston instance over a period of early Sept - late Oct 2018.  

Unfortunately, since Preston was not running before the EML file was orphaned/ removed, we do not have a copy of it somewhere. Also, I am not aware of a method to retrieve this historic content via some other openly available method / service. Another theory is that the GBIF team is relocating the archive associated with collection with id af32ab2e-7be6-42ca-a570-ad79fe0e32bb , and is in the process of setting up a new IPT (integrated publishing toolkit) instance. Without a configuration history associated with the dataset/collection with key af32ab2e-7be6-42ca-a570-ad79fe0e32bb (see also https://www.gbif.org/dataset/af32ab2e-7be6-42ca-a570-ad79fe0e32bb), we don't know how configuration or associated content changed over time, simply because this content is not being tracked in an open manner. From the available metadata, the dataset was first published in 2016. From this information, the longevity, or availability period, of the dataset was about 2 years. Extending this simple exampl e, a more continuous and widescale monitoring scheme can be constructed to monitor the health of our digital datasets. Also, by employing content tracking techniques, we have an effective tool to stave of natural phenonemona in our digital infrastructures: linkrot and datarot.   
