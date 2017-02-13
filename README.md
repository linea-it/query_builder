# `query_builder`

The `query_builder` was developed to automate SQL operations based on input data and configuration provided by the user. The concept behind the `query_builder` is that a complex operation in a relational DB can be optimized by breaking it down into smaller operations. These operations can be native DB operations like `join`, bitwise operations or extended operations defined by the user. As a result of an operation, intermediate tables are created. These tables can be permanent in the database or trully temporary. The job of the query builder is to combine those intermediate tables in a predefined order to reduce the overall execution time. For instance, time consuming operations like `join` between large tables are performed at the end only when the size of the involved tables are reduced by previous operations. 

The main concepts of the `query_builder` implementation are:

**Workflow:** the sequence of operations is described in a workflow like representation, the `query_builder` builds the queries for each operation, create intermediat tables and combine them in a predefined order to optimize the execution time. It also manages the parallelization of independent operations that can be run simultaneously.

**Operation:**  a relational DB operation or a new operation defined by the user combining other operations. The result of an operation is an SQL query which is built based on input tables, intermediate tables created in previous operation of the workflow and on the configuration provided by the user for each operation. 

New operations can be easily added and specified in the JSON file which represents the workflow. A new operation is defined overriding the `get_statement` method in the `IOperation` class. All operations are defined in Python using the SQLAlchemy core library.

**Intermediate table:** is a table created on the DB to store temporary data that are used to compute the final result. These tables can either be 'permanent' or 'temporary' depending on the configuration specified by the user. Permanent tables are useful for gather information on intermediate steps like fraction of rows removes, and other dianostics.


Example of a simple workflow:

(Lucas, inclui aqui um exemplo de workflow e descreve o que ele faz passo a passo)



# Running the `query_builder` locally

1. Clone the project, create a virtualenv and install dependencies
```
  git clone https://github.com/lucasdpn/query_builder.git

  cd query_builder

  virtualenv env -p python3
  source env/bin/activate
  pip install -r requirements.txt
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
