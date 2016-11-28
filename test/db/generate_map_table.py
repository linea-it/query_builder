from sqlalchemy import (create_engine, Table, Column, Integer, Float, MetaData,
                        ForeignKey)
from sqlalchemy.sql import select
from sqlalchemy.schema import CreateTable

import os
import sys

# PYTHONPATH=<base_project_dir> python file.py
import settings


eng = create_engine(settings.str_connection(settings.DATABASE))
with eng.connect() as con:
    meta = MetaData(eng)

    map_table = Table('map_table', meta,
                      Column('pixel', Integer, primary_key=True),
                      Column('signal', Float),
                      Column('ra', Float),
                      Column('dec', Float))

    CreateTable(map_table)
    meta.create_all(eng)

    data = ({"pixel": 1, "signal": 0.1, "ra": 100.1, "dec": 200.1},
            {"pixel": 2, "signal": 0.2, "ra": 100.2, "dec": 200.2},
            {"pixel": 3, "signal": 0.3, "ra": 100.3, "dec": 200.3},
            {"pixel": 4, "signal": 0.4, "ra": 100.4, "dec": 200.4},
            {"pixel": 5, "signal": 0.5, "ra": 100.5, "dec": 200.5},
            {"pixel": 6, "signal": 0.6, "ra": 100.6, "dec": 200.6},
            {"pixel": 7, "signal": 0.7, "ra": 100.7, "dec": 200.7},
            {"pixel": 8, "signal": 0.8, "ra": 100.8, "dec": 200.8},
            {"pixel": 9, "signal": 0.9, "ra": 100.9, "dec": 200.9},
            {"pixel": 10, "signal": 1.0, "ra": 101.0, "dec": 201.0},
            {"pixel": 11, "signal": 1.1, "ra": 101.1, "dec": 201.1})

    con.execute(map_table.insert(), data)
