## OLAP
It simulates OLAP-cube, but not OLAP
The are 3 parts to make

### Structure
Structure is created by toml files

You need to provide path for OlapStructureGenerator.class with folder for one OLAP structure:

```
BASE_PATH/ # Folder for one OLAP Structuce
├─ data/ # Contains *.toml files with structure for data-tables
│ ├─ file.toml # Can have any name
│ └─ ...
├─ dimension/ # Contains *.toml files with structure for dimension-tables
│ ├─ file.toml # Can have any name
└─└─ ...
```

<b>Structure for toml file for data-table</b>
```
table = "table_name"
schema = "schema_name"
database = "database_name"
base_table = "true" # true for table without any aggregations

[fields]
field_name = {field_type = "field_type", alias = "alias", calculation_type = "none", following_calculation = "none", front_name = "none"}
```
```field_type``` — one of ```OlapFieldTypes```

```alias``` should be specified for any field. It is used for joins and calculations search

```calculation_type``` should be one of ```OlapCalculations```. "none" if no calculation was made

```following_calculation``` should be one of ```OlapFollowingCalculations```. "none" if ```calculation_type``` == "none"
or no further calculation can be used on field

```front_name```

<b>Structure for toml file for dimension-table</b>
```
table = "table_name"
schema = "schema_name"
database = "database_name"

[fields]
sk_id = {field_type = "service_key", alias = "developer_name", front_name="none"}
developer_name = {field_type = "dimension", alias = "none", front_name="front_name"}
```
```field_type``` should be one of ```[OlapFieldTypes.SERVICE_KEY.value, OlapFieldTypes.DIMENSION.value]``` <br>
You have to have one ```field_type``` = "service_key"<br>
"none" (non-case-sensitive) will be turned to pythonic ```None```
