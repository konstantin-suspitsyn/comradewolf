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

Structure for toml file for data-table
```

```

Structure for toml file for dimension-table
```
table = "table_name"
schema = "schema"
database = "database"

[fields]
sk_id = {field_type = "service_key", alias = "developer_name", front_name="none"}
developer_name = {field_type = "dimension", alias = "none", front_name="front_name"}
```
field_type should be one of ```[OlapFieldTypes.SERVICE_KEY.value, OlapFieldTypes.DIMENSION.value]``` <br>
You have to have one field_type = "service_key"<br>
"none" (non-case-sensitive) will be turned to pythonic ```None```
