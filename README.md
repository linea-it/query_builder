# `query_builder`

The `query_builder` was developed to automate SQL operations based on input data and configuration provided by the user. The concept behind the `query_builder` is that a complex operation in a relational DB can be optimized by breaking it down into smaller operations. These operations can be native DB operations like `join`, bitwise operations or extended operations defined by the user. As a result of an operation, intermediate tables are created. These tables can be permanent in the database or trully temporary. The job of the query builder is to combine those intermediate tables in a predefined order to reduce the overall execution time. For instance, time consuming operations like `join` between large tables are performed at the end only when the size of the involved tables are reduced by previous operations.

The main concepts of the `query_builder` implementation are:

**Workflow:** the sequence of operations is described in a workflow like representation, the `query_builder` builds the queries for each operation, create intermediat tables and combine them in a predefined order to optimize the execution time. It also manages the parallelization of independent operations that can be run simultaneously. The workflow is defined in a .dot file which stores the dependencies between the operations.

**Operation:**  a relational DB operation or a new operation defined by the user combining other operations. The result of an operation is an SQL query which is built based on input tables, intermediate tables created in previous operation of the workflow and on the configuration -a JSON file -provided by the user for each operation.
A new operation is defined overriding the `get_statement` method in the `IOperation` class and it is defined using the SQLAlchemy core library.

**Intermediate table:** is a table created on the DB to store temporary data that are used to compute the final result. These tables can either be 'permanent' or 'temporary' depending on the configuration specified by the user. Permanent tables are useful for gather information on intermediate steps like fraction of rows removes, and other diagnostics.

**Example of a simple workflow:**
As told before, the wokflow is defined in a .dot file and the configurations for each operation, in a JSON file. A new operation is declared as a new JSON pair. The string defines the name of the operation beeing used by the .dot file to describe the workflow, while the value must contain an Json object with at least the fields:
**"op"** - Is the type of the operation that will be executed. The operation must be defined in the `operations` file.
**"permanent_table"** - If true, the table will be kept on the database. Otherwise, it will be removed.

Other fields can be added accordingly the operation needs. For example, the exposure_time_i operation has a field "value" that will be used by the operation "great_equal" only. To access this fields the method get_statement has an dictionary attribute called `params`.

In this sample, the exposure_time uses the intermediate tables produced by exposure_time_i and exposure_time_r. To access this data, the `get_statement` method has the attribute sub_operations in which the key is the operation name and the value associated is an object representation of an intermediate table.

**Workflow definition:**
exposure_time -> exposure_time_i
exposure_time -> exposure_time_r

**Configuration definition:**
{
  "exposure_time"{
    "op": "join",
    "permanent_table": true,
  },
  "exposure_time_i"{
    "op": "great_equal",
    "permanent_table": false,
	"schema":"systematic_maps",
    "db": "y1a1_coadd_cosmos_d04_4096_exptime_i_10023575",
    "value": "0.55"
  },
  "exposure_time_r"{
    "op": "great_equal",
	"permanent_table": false,
    "schema":"systematic_maps",
    "db": "y1a1_coadd_cosmos_d04_4096_exptime_r_10023575",
    "value": "0.33"
  }
}

The arguments received by the `get_statement` function for the example above is:

![alt tag](https://cloud.githubusercontent.com/assets/6139408/24215930/232311ce-0f19-11e7-94cf-77a697a27569.png)

This way, all the operations can be written overriding the method `get_statement`.

# Running the `query_builder` locally

1. Clone the project, create a virtualenv and install dependencies
```

  sudo apt-get install python3-pip virtualenv git tcl-dev tk-dev python-tk python3-tk

  git clone https://github.com/lucasdpn/query_builder.git

  cd query_builder
  virtualenv --no-site-packages --always-copy --python python3 env
  source env/bin/activate
  export CFLAGS=-fPIC
  pip3 install --upgrade setuptools pip virtualenv
  pip3 install --upgrade -r requirements.txt
  git submodule init
  git submodule update
```

2. install postgresql

3. create databae
```
  CREATE DATABASE query_builder
```

4. create schema to save the results
```
  CREATE SCHEMA tst_oracle_output
```

5. download testing data and import in the database - sub_set_y1a1.sql has a set of data from y1a1_coadd_cosmos_d04 and others pypelines.
```
  download sub_set_y1a1.sql from http://devel2.linea.gov.br/~lucas.nunes/
```

6. import data in local database
```
  psql -U postgres query_builder < sub_set_y1a1.sql
```

7. execute tests (optional)
```
  PYTHONPATH=. python test/test_queries_y1ay_subset.py
```
