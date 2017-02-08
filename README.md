# query_builder

The ***query_builder*** was developed to automate SQL operations based on input data provided by the user. The concept behind the ***query_builder*** is that a complex operation in a relational DB can be optimized by breaking it down into smaller operations that create intermediate tables which are combining in a predefined order reducing  the overall execution time.

New SQL operations can be easily added and specified in the input JSON file which also represents the workflow. A new operation - query - is defined overriding the interface IQuery, method get_statement. The SQLAlchemy core must be used to define the operations. 

So, given a input JSON file and all the queries descriptions, all the operations are created automatically having a set of useful methods and they are accessed through a dictionary where the key is the operation name defined in the JSON file and the value is the object associated with the operation.

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

4. download testing data and import in the database - sub_set_y1a1.sql has a set of data from y1a1_coadd_cosmos_d04 and others pypelines.
```
  download sub_set_y1a1.sql from http://devel2.linea.gov.br/~lucas.nunes/
```

5. import data in local database
```
  psql -U postgres query_builder < sub_set_y1a1.sql
```

6. execute tests (optional)
```
  PYTHONPATH=. python test/test_queries_devel2.py
```
