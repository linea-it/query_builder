# query_builder

The ***query_builder*** was developed to automate SQL operations based on input data provided by the user. The concept behind the ***query_builder*** is that a complex operation in a relational DB can be optimized by breaking it down into smaller operations that create intermediate tables which are combining in a predefined order reducing  the overall execution time. Besides, we are not only concerned in the final results, but we wish to be able to analise the intermediate steps.

To achieve this purpose, the code uses two concepts:
***Operation*** - A query that is built based on the input data and optionally, it can depend on intermediate tables.
***Intermediate table*** - "is a table created on the database to store temporary data that are used to calculate the final result set. These tables can either be 'permanent' or 'temporary' depending on the configuration of it."

New SQL operations can be easily added and specified in a input JSON file which also represents the workflow. A new operation is defined overriding the interface IOperation, method get_statement. The SQLAlchemy core must be used to define the operations. 

So, given a input JSON file, the query_builder builds all the operations, managing the construction in the right order allowing parallelism.

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
