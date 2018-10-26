# Preston Analysis

Preston records how and when datasets are discovered and accessed in the rdf/nquads. These crawl records, or biodiversity dataset graphs, can be loaded into triple stores like [Apache Jena Fuseki](https://jena.apache.org/documentation/fuseki2/) for discovery and analysis. 

This page contains some [sparql](https://www.w3.org/TR/rdf-sparql-query/) queries to discover and analyze the dataset graph 

## change rate of url

URLs are used to access content. While the URLs might be static, the content is often not. This query generates a list of urls in decreasing order of change rate. 

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



