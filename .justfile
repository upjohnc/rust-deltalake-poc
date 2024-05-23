default:
    just --list

# create python virtualenv
create-venv:
    python -m venv .venv
    # need to manually set the virtualenv
    # can't do it with just because it uses a
    # subshell.
    # command: `source .venv/bin/activate`


# install packages
pip-install:
    pip install -r requirements.txt

# read delta table:
read-delta version="1":
    python polars_read.py {{ version }}
