![Build status](https://travis-ci.org/minibigio/miniduke.svg?branch=master)
# Welcome
This project is a Elasticsearch plugin made for our appliance ReconIA. For more information, please see: ...
We made this to find links and deduplicate values in a set of entries. To work, Miniduke is able to make a Master Data Managment (MDM) to enrich data.

### What does it mean ?
You can find data even with errors or typos. See the Getting Started page (https://github.com/minibigio/miniduke/wiki/Getting-started) for more information.

### How does it work ?
Basically, it uses probabilities to check if two values are identical or not. Miniduke is based on an other project called Duke (https://github.com/larsga/Duke).

### Free
This is a free and open source project. You can also contribute (https://github.com/minibigio/miniduke/wiki/Contribute) to the project !

## First steps
- Clone the project
- Build the project `gradle build -x test` (to build with tests, please refer to the [contributing page](https://github.com/minibigio/miniduke/wiki/Contribute))
- Install the plugin
`./bin/elasticsearch-plugin install file://miniduke-x.x-SNAPSHOT.zip`
- Set the pipeline

```
PUT _ingest/pipeline/miniduke
{
  "description": "miniduke",
  "processors": [
    {
      "miniduke": {
        "fields": "fields",
        "threshold": "threshold",
        "thresholdMaybe": "thresholdMaybe",
        "weight": "weight",
        "data": "data",
        "comparator": "comparator",
        "filters": "filters",
        "host": "host"
      }
    }]
}
```

- Index your entries
```
POST index_name/mdm?pipeline=miniduke
{
  "fields": ["first_name", "last_name", "mail", "birth"],
  "weight": [[0.5, 0.8], [0.5, 0.8], [0.5, 0.8], [0.5,0.8]],
  "data": ["Marc", "Babouin", "marcb@minibig.io", "1982-06-08"],
  "comparator": ["no.priv.garshol.duke.comparators.Levenshtein",
                "no.priv.garshol.duke.comparators.Levenshtein",
                "io.minibig.miniduke.comparators.EmailComparator",
                "io.minibig.miniduke.comparators.DateComparator"],
  "filters": ["first_name"],
  "threshold": 0.82,
  "thresholdMaybe": 0.7
  "host": "localhost:9200"
}
```

Magic, the values are automatically reconcilied !
