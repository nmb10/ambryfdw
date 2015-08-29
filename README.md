# Ambry partition msgpack files Foreign Data Wrapper (multicorn)

This FDW can be used to access data stored in the ambry partitions packed with msgpack. Each column defined
in the table will be mapped, in order, against columns in the ambry partition file.

## Installation.
```bash
pip install ambryfdw
```

## Run tests.
```bash
python setup.py test
```

## Options

filename (required)
    The full path to the CSV file containing the data. This file must be readable to the postgres user.

# Usage
Supposing you want to parse the following msgpack partition file, located in ``/tmp/test.msg``::

    Year,Make,Model,Length
    1997,Ford,E350,2.34
    2000,Mercury,Cougar,2.38

You can declare the following table:
```sql
    CREATE SERVER partition_srv foreign data wrapper multicorn options (
        wrapper 'ambryfdw.PartitionMsgpackForeignDataWrapper'
    );

    CREATE FOREIGN TABLE partition_test (
           year numeric,
           make character varying,
           model character varying,
           length numeric
    ) SERVER partition_srv OPTIONS (
           filename '/tmp/test.msg');

    SELECT * FROM partition_test;
```
```bash
     year |  make   | model  | length
    ------+---------+--------+--------
     1997 | Ford    | E350   |   2.34
     2000 | Mercury | Cougar |   2.38
    (2 lines)
```
