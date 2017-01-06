from sqlalchemy import (create_engine, Table, Column, Integer, Float, MetaData,
                        ForeignKey)
from sqlalchemy.sql import select
from sqlalchemy.schema import CreateTable

import os
import sys

# PYTHONPATH=<base_project_dir> python file.py
from utils import db_connection
import settings
from utils.db import dal, str_connection


def _template_create(table, data):
    CreateTable(table)
    dal.metadata.create_all(dal.engine)

    with dal.engine.connect() as con:
        con.execute(table.insert(), data)


def systematic_maps():
    map_table = Table('systematic_map', dal.metadata,
                      Column('pixel', Integer, primary_key=True),
                      Column('signal', Float),
                      Column('ra', Float),
                      Column('dec', Float))

    data = ({"pixel": 1, "signal": 0.1, "ra": 100.1, "dec": 200.1},
            {"pixel": 2, "signal": 0.2, "ra": 100.2, "dec": 200.2},
            {"pixel": 3, "signal": 0.3, "ra": 100.3, "dec": 200.3},
            {"pixel": 4, "signal": 0.4, "ra": 100.4, "dec": 200.4},
            {"pixel": 5, "signal": 0.5, "ra": 100.5, "dec": 200.5},
            {"pixel": 6, "signal": 0.6, "ra": 100.6, "dec": 200.6},
            {"pixel": 8, "signal": 0.8, "ra": 100.8, "dec": 200.8},
            {"pixel": 10, "signal": 1.0, "ra": 101.0, "dec": 201.0},
            {"pixel": 11, "signal": 1.1, "ra": 101.1, "dec": 201.1})

    print ("Creating systematic maps")
    _template_create(map_table, data)


def systematic_maps_2():
    map_table = Table('systematic_map_2', dal.metadata,
                      Column('pixel', Integer, primary_key=True),
                      Column('signal', Float),
                      Column('ra', Float),
                      Column('dec', Float))

    data = ({"pixel": 1, "signal": 0.1, "ra": 100.1, "dec": 200.1},
            {"pixel": 3, "signal": 0.3, "ra": 100.3, "dec": 200.3},
            {"pixel": 5, "signal": 0.5, "ra": 100.5, "dec": 200.5},
            {"pixel": 7, "signal": 0.7, "ra": 100.7, "dec": 200.7},
            {"pixel": 9, "signal": 0.9, "ra": 100.9, "dec": 200.9},
            {"pixel": 11, "signal": 1.1, "ra": 101.1, "dec": 201.1})

    print ("Creating systematic maps 2")
    _template_create(map_table, data)


def bad_regions():
    table = Table('bad_regions_map', dal.metadata,
                  Column('pixel', Integer, primary_key=True),
                  Column('signal', Integer),
                  Column('ra', Float),
                  Column('dec', Float))

    data = ({"pixel": 1, "signal": 1, "ra": 100.1, "dec": 200.1},
            {"pixel": 2, "signal": 2, "ra": 100.2, "dec": 200.2},
            {"pixel": 3, "signal": 4, "ra": 100.3, "dec": 200.3},
            {"pixel": 4, "signal": 8, "ra": 100.4, "dec": 200.4},
            {"pixel": 5, "signal": 16, "ra": 100.5, "dec": 200.5},
            {"pixel": 6, "signal": 32, "ra": 100.6, "dec": 200.6},
            {"pixel": 7, "signal": 64, "ra": 100.7, "dec": 200.7},
            {"pixel": 8, "signal": 128, "ra": 100.8, "dec": 200.8},
            {"pixel": 9, "signal": 255, "ra": 100.9, "dec": 200.9},
            {"pixel": 10, "signal": 0, "ra": 101.0, "dec": 201.0},
            {"pixel": 12, "signal": 63, "ra": 101.2, "dec": 201.2})

    print ("Creating bad region maps")
    _template_create(table, data)


def create_all_tables():
    systematic_maps()
    bad_regions()


def delete_all_tables():
    dal.metadata.drop_all(dal.engine)


if __name__ == '__main__':
    dal.db_init(str_connection())
    print ("Creating systematic maps")
    systematic_maps()
    print ("Creating systematic maps 2")
    systematic_maps_2()
    print ("Creating bad region maps")
    bad_regions()
