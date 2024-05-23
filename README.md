# Rust + Delta Lake

A poc project to show how to work with delta tables from rust code.

## Running

Assuming rust is set up locally, the command is simply:

`cargo run <source_number>`

The `source_number` is the file number which are in the `source_data` directory.

This code will:
- build the delta table (version 0) if the table isn't in the `delta` directory.
- read the source_data parquet file and append to the delta table

## Python: reading the delta table

To view the data in the delta table, there is a python script.
It will print the table.  It can also take in a version number,
which will be used as the delta table version number.  If the
rust code is run three times (one for each of the source files)
then the number of possible version numbers is 3.

**Running script**
- `just  create-venv`
- `source .venv/bin/activate`
- `just pip-install`
- `just read-delta`

