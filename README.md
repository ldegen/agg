# The Aggregator

The Aggregator processes tabular data into documents. It does so by
*aggregating* (hence the name) consecutive rows that represent the different
parts of one document.

## Install

The usual `npm install -g agg` should do the trick.

## Command Line Interface

**WARNING**: until the 1.0 release, We will probably change a few things in the
CLI. We are trying hard to keep the existing options as they are, though.

Usage:

      agg [options] [csv file]

If no `csv file` is given, CSV-Data is read from STDIN. JSON documents are
written to STDOUT, unless the `-I` option is used, in which case the documents
are directly uploaded into ElasticSearch.

Valid options are:

### `-k <attr>`

Name of the primary key attribtue to be used with the `-s` and `-I` options.
Defaults to `id`.

### `-v <attr>`

Used together with the `-s` option to inline the given "value"-Attribute. I.e.
if your mapping would normaly generate documents of the form
`{"id": 42,"value": "The value"}`, and you are using the `-s` option, the
output will be of the form `{"42": "The value", ... }`

### `-s`

Reduce output to a single JSON Object, using a designated primary key attribute
as key. This defaults to `id`, you can override this by using the `-k` option.

By default, the structure of your documents will not be changed. In particular,
they will include the primary key. See the `-v` option for inlining a single
value attribute.

### `-T <path>`

Apply a custom transformation. The file will be loaded using `require`. It is
expected to contain a node.js module that exports a single factory function.
This factory is expected to produce something that works like a
`stream.Transform`. The output of the aggregation phase will be piped through
this transform.

### `-F <path>`

Same as `-T`, but will be inserted into the pipeline before the aggregation
phase. It can be usefull to preprocess/filter the parsed CSV-data.

### `-L <path>`

Used in conjunction with `-T` and `-F` to provide the custom transform with
arbitrary secondary data, typically a JSON file containing lookup-tables or
similar. The file will be loaded using `require` and the result will be passed
as an argument to the factory function when creating the custom transform
instance.

### `-b`

Create output that can be used as body for an ElasticSearch bulk index request.
Without this option, the tool will write one JSON object per document to
STDOUT, separated by newlines. *With* this option, however, the documents will
be interleaved with command metadata interpreted by the ElasticSearch bulk API.

### `-I <index>`

If this option is given, documents are not written to STDOUT, but will be
uploaded to a local ElasticSearch node using the given name as target index.
Implies `-b`.

### `-S <fixed pk>, --S=<fixed pk>`

When you want to aggregate all data into a single document but still want to
index this document using some apriori known primary key, use this option.

It behaves like `-b -s` but allows you to specify a primary key for the
document.

### `-t <type>`

Used in conjunction with `-I` to specify the document type. Defaults to
`project`, for historic reasons.

### `-h <host>`

Used in conjunction with `-I` to specify the ElasticSearch node to use.
Defaults to `http://localhost:9200`.

## Mapping Columns to Attributes

Columns in the input are mapped to leaf attributes in the output. The mapping
is determined by the aggregator by parsing the column label. Each column label
describes the path from the document root down to the attribute for which the
values in the respective columns are to be used. The path is given as attribute
names separated by dots (`.`): For example this:

| value   | name.de   | name.hu  |
| ------- | --------- | ---------|
| 1       | Eins      | Egy      |
| 2       | Zwei      | Kettő    |
| 3       | Drei      | Három    |

Will produce this:

``` json
  {"value":1, "name":{"de": "Eins", "hu": "Egy"}}
  {"value":2, "name":{"de": "Zwei", "hu": "Kettő"}}
  {"value":3, "name":{"de": "Drei", "hu": "Három"}}
```

The non-leaf attributes or inner attributes are called document parts. Any
attribute or part can either be single-valued or multi-valued. To mark it as
multi-valued, append `[]` to its name. Here is an example for a multi-valued
part:

| value   | names\[\].lang   | names\[\].string  |   
| ------- | ---------------- | ------------------|
| 1       | de               | Eins              |
|         | hu               | Egy               |
| 2       | de               | Zwei              |
|         | hu               | Kettő             |
| 3       | de               | Drei              |
|         | hu               | Három             |

This will produce the following output:

``` json
   {"value": 1, "names": [{"lang": "de", "string": "Eins"}, {"lang": "hu", "string": "Egy"}]}
   {"value": 2, "names": [{"lang": "de", "string": "Zwei"}, {"lang": "hu", "string": "Kettő"}]}
   {"value": 3, "names": [{"lang": "de", "string": "Drei"}, {"lang": "hu", "string": "Három"}]}
```

Multi-Valued parts are normally represented as JSON arrays. If you want an
associative array (a.k.a. dictionary or hash table) instead, you must pick a
single leaf attribute and mark it as key-attribute. You can do so by appending
a `#` to its name. So in the above example we could have used `names[].lang#`
to produce:

``` json
   {"value":1, "names":{"de": {"lang":"de","string":"Eins"}, "hu":{"lang":"hu","string":"Egy"}}}
   {"value":2, "names":{"de": {"lang":"de","string":"Zwei"}, "hu":{"lang":"hu","string":"Kettő"}}}
   {"value":3, "names":{"de": {"lang":"de","string":"Drei"}, "hu":{"lang":"hu","string":"Három"}}}
```

In this particular case, it seems a bit clumbsy to still include the `lang` and
`string` keys in our dictionary, when actually we simple want a simple map from
language to translated string. In such cases, we can tell the aggregator to
replace the dictionary entries with one of their attributes. We also call this
'inlining'. To do this, just append the name of the attribute you want to
inline *after* the `#`. In our example, the header line would look like this:

value names\[\].lang\#string names\[\].string ------- ------------------------
------------------

With the same values as before, the result would look like this:

``` json
   {"value":1, "names":{"de": "Eins", "hu":"Egy"}}
   {"value":2, "names":{"de": "Zwei", "hu":"Kettő"}}
   {"value":3, "names":{"de": "Drei", "hu":"Három"}}
```

Note that the same can be achieved on the document toplevel by using the `-s`,
`-k` and `-v` command line options.

## Wildcard Attribute Mappings

Depending on your use case you may run into situations where a single column
contains values that conceptually belong to different parts, depending on the
context of the rows that contains that values. A typical example would be
generic attributes that are shared by all document parts. For instance, the
GEPRIS index document parts all contain the attributes `partType`,
`partDeleted` and `serialNo`.

To support those situations, the aggregator lets you specify so-called wildcard
attributes. Wildcard column mappings are always of the form `*.attributeName`.
A wildcard can be used as primary key in a multi-valued part, but it cannot be
multi-valued itself, and it cannot be nested or contain other attributes or
parts.

By default, wildcard attributes are added to the inner-most part(s) that
receive any contribution by a particular role. So this:

| \*.row   | id+   | persons\[\].id   | persons\[\].role   | title.de            | title.en          |
| -------- | ----- | ---------------- | ------------------ | ------------------- | ----------------- |
| 1        | 100   | 101              | 'foo'              |                     |                   |
| 2        | 100   | 102              | 'foo'              |                     |                   |
| 3        | 100   |                  |                    | 'Deutscher Titel'   | 'English Title'   |

will produce this:

``` json
  {
    "id":100,
    "persons":[{
      "row":1,
      "id":101,
      "role":"foo"
    },{
      "row":2,
      "id":102,
      "role":"foo"
    }],
    "title":{
      "row":3,
      "de":"Deutscher Titel",
      "en":"English Title"
    }
  }
```

Here, the the root document itself didn't get any `row`-Attribute, because all
three rows contributed to parts that were nested within the document.

Since this is not always desired, a special syntax can be used to explicitly
state to which document part a contribution actually should go. For example:

| title.short.de   | title.short.en   | \*.sn   |
| ---------------- | ---------------- | ------- |
| Der Titel        | The Title        | 42      |

The aggregator would interprete this as a contribution to the part
`title.short`. This, this part will not only get the attributes `de` and `en`,
but also the wildcard attribute `sn`.

If we change the labels to read

| title:short.de   | title:short.en   | \*.sn   |
| ---------------- | ---------------- | ------- |
| Der Titel        | The Title        | 42      |

The aggregator will read the as a contribution to the part title, and put the
`sn` there. You could even write `:title.short.de` to make the wildcard
attributes go all the way up to the toplevel.

Note that while adding wildcard attributes to multi-valued *parts* is no
problem, it is simply not possible for leaf attributes to receive any wildcards
attributes, no matter if they are multi-valued or single-valued. Take this
example:

| id+   | \*.wc   | multi\[\]   |
| ----- | ------- | ----------- |
| 10    | 1       | a           |
| 10    | 2       | b           |
| 10    | 3       |             |

In the first two lines, values are contributed to document root and to the
multi-valued *leaf* attribute`multi[]`. Since the latter is nested within the
former, the aggregator ignores the contribution to the document root. On the
other hand, since `multi[]` is a leaf attribute, it cannot receive additional
attributes. So in this case, the wildcard is silently ignored because there is
no valid target to receive it. The third line only contribues to the document
root, so this time the wildcard attribute is processed. The resulting document
looks like this:

{ "id":10, "wc":3, "multi":\["a","b"\] }

## Aggregation Semantics

The intersting part in the whole aggregation process is deciding which rows
belong together and when to start a new part or even a new document. The
algorithm that makes this decission is written by two simple rules:

    Empty or null-Values are *always* ignored.

    Any part containing a single-valued attribute can not hold more than one value for
    that attribute.

For each part type, the aggregator maintaince a reference to the particular
part instance that most recently received a contribution. If a value for some
single-valued attribute is encountered, and the current part that would
normally receive this attribute value already has a value for that attribute,
this means one of two things:

-   a new instance of that parttype must be started to contain the new value.

-   the old and the new value are the same and the attribute is *known* to
    contain consecutive identical values. A most prominent example for this
    kind of "unique" attributes is the document primary key, or any other
    attribute marked with a `#`. You can also suffix leaf attributes with a `+`
    to tell the aggregator that they may contain consecutive identical values
    within the same part if you do not want the special behaviour associated
    with the `#` attributes.

If the respective part happens to be the document root, the current document is
committed and a new one is started. Otherwise only the current part is comitted
and a new part of the same type is created to receive the new value. If a
document part is committed, all nested parts, that are "active", i.e. recieved
contributions since the part was created, are comitted as well.

And this is basically how the whole thing works.

## A Note on Ordering

It is important to keep in mind that while the column ordering has no effect on
the output (the aggregator automatically finds a 'good' processing order), the
order of the rows is *very* important. *You* have to make sure that all rows
belonging to the same document part are kept together, because the aggregator
*cannot* do this for you.
