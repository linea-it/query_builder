# query_builder

The {\verb query_builder } was developed to automate SQL operations based on input data provided by the user. The concept behind the {\verb query_builder } is that a complex operation in a relational DB can be optimized by breaking it down into smaller operations that create intermediate tables which are combining in a predefined order reducing  the overall execution time.

New SQL operations can be easily added and specified in the input JSON file which also represents the workflow. A new operation - query - is defined overriding the interface IQuery, method get_statement. The SQLAlchemy core must be used to define the operations. 

So, given a input JSON file and all the queries descriptiont, all the operations are created automatically having a set of useful methods and they are accessed through a dictionary where the key is the operation name and the value is the object associated with the operation. 

1. Clone the project, create a virtualenv and install dependencies
```
  git clone https://github.com/lucasdpn/query_builder.git

  cd query_builder

  virtualenv env -p python3
  source env/bin/activate
  pip install -r requirements.txt
```
