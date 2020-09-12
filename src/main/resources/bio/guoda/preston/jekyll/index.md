---
layout: default
---
<h1>Welcome to Content-based iDigBio!</h1> 

The following table was created by:

1. creating a new blank Jekyll site:
```
jekyll new [site_dir] --blank
```
1. archiving (meta-)data and media of an iDigBio registered [UAM Insect Collection (Arctos)](https://www.idigbio.org/portal/recordsets/eaa5f19e-ff6f-4d09-8b55-4a6810e77a6c) running 
```
cd [site_dir]
preston track https://search.idigbio.org/v2/search/records/?rq=%7B%22recordset%22%3A+%22eaa5f19e-ff6f-4d09-8b55-4a6810e77a6c%22%7D
``` 
using iDigBio API (~3.2GB json/images) and [Preston](https://preston.guoda.bio). 
3. generating a static Jekyll site content from Preston archive using:
```
preston copyTo --type jekyll .
```
4. launch website using:
```
jekyll s
``` 
5. opening generated static site in browser at 
```
http://localhost:4000
```
6. (optional) register content with content registeries (e.g., https://hash-archive.org) to let others know that you are hosting media. Example: media associated with mediarecord [ac13f15c-a28a-4d74-8c25-cdacb8a78da9](https://www.idigbio.org/portal/mediarecords/ac13f15c-a28a-4d74-8c25-cdacb8a78da9) as [hash-archive](https://hash-archive.org/sources/hash://sha256/103c3f461a3f477da8b20fc34db5bd142dd890dad4c1a0bcd4ffa6f9070a8fa7).

  <table>
    <thead>
    <tr>
      <th>online (location-based)</th>
      <th>offline (content-based)</th>
    </tr>
    </thead>
    <tdata>
        {%- for page in site.pages limit:10 -%}
        {%-   if page.idigbio.type == "mediarecords" -%}
        {%      assign mediarecord = page.idigbio %}
        {%      include media.html mediarecord=mediarecord %}
        {%-   endif -%}
        {%- endfor -%}
    </tdata>
  </table>