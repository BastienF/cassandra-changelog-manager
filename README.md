# Cassandra-Changelog-Manager

## Executive summary

Cassandra-Changelog-Manager [CCM] is a tool that provides a way to manage cassandra schemas with incremental evolutions.

CCM will take in input a list of ordered .cql files and execute them against one or many keyspaces, the right execution of these files will be persisted in a specific administration keyspace. 

The parameters to provide to CCM are:
 * The cassandra cluster connection configuration (via a config file)
 * The path of config file as JVM parameter (-Dconfig=<path>)
 * The path of directory containing all the .cql files as JVM parameter (-Dchangelog=<path>)

## Build the application

`sbt assembly`

## Run the application

### Create cassandra connection config file
Create a config file with the following content :
```
conf {
  cassandra {
    hostnames = ["localhost"]
    port = 9042
    ssl = false
    admin_keyspace = "keyspace_administration"
    keyspaces = ["keyspace1", "keyspace2"]
    username=adm
    password=password
    app_version="application_name"
  }

  changelog_applier {
    altered_scripts = ["alteredScript.cql"]
  }
}

```

* `cassandra.keyspaces` : specify the list of all keyspaces on which execute the cql changelog. In most cases you will provide only one keyspace. If you provide multiple keyspaces the cql files will be executed on all those keyspaces 
* `cassandra.admin_keyspace` : specify the name of the administration keyspace in which the `schema_version` table will be created. The admin keyspace can be the same than the main keyspace if the `schema_version` table is not in conflict with an existing table.
* `cassandra.app_version` : The unique identifier of your application, multiple application (or application versions) can share the same administration keyspace and the same `schema_version` table. 
* `changelog_applier.altered_scripts`: Optionnal parameter that allow you to change the md5sum of an already executed file without getting an error. (CCM will execute only the not already executed cql files but will check that the old cql files have not been modified)

### Run changelog execution

`java -Dconfig=</path/to/config.conf> -Dchangelog=</path/to/changelog/dir/> -Dcassandra.password=<"password"> -jar ./cassandra-changelog-manager.jar`

* `cassandra.password` can be specified directly into the config file
* `changelog` is the path to the directory containing all the cql files. See : _Changelog directory content_ section 

### Changelog directory content
The cql files present inside the `changelog` will be executed by CCM on alphabetical order. A good way to handle this is to prefix all your scripts by a large padded index.
Example :
```
./path/to/changelog
│   00000000000000_schema_init-tables.cql
│   00000000000001_schema_alter-some-tables.cql
│   ...
│   00000000000033_schema_create-new-tables.cql

    
```

Optionnaly the changelog directory can contains subdirectories named `v[0-9]+` to allow reinit of the schemas.
CCM will exectude only the last `v` directory content
```
./path/to/changelog
└───v0
│   │   00000000000000_schema_init-tables.cql
│   │   00000000000001_schema_alter-some-tables.cql
│   │   ...
│   │   00000000000033_schema_create-new-tables.cql
│
└───v1
    │   00000000000000_schema_reinit-all-schemas.cql
    │   00000000000001_schema_add-some-new-tables.cql
    │   ...
    
```
 
### Changelog cql files format
Each cql files in the `changelog` directory have to contain only valid CQL instructions.
Each cql commandes should be preceded by the comment `-- cassandra-changelog-manager statement`
Example :
```
-- cassandra-changelog-manager statement
CREATE TYPE IF NOT EXISTS sometype (
    eventid uuid,
    eventtype text,
    date timestamp
);

-- cassandra-changelog-manager statement
CREATE TABLE IF NOT EXISTS testtable (
    date timestamp,
    idtable int,
    eventid uuid,
    value text,
    PRIMARY KEY ((date, idtable), eventid)
)
```

## Requirements

### Building the application
* SBT
* JDK8

### Running the application
* JRE8
