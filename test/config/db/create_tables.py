from sqlalchemy import (create_engine, Table, Column, Integer, Float, MetaData,
                        ForeignKey)
from sqlalchemy.sql import select
from sqlalchemy.schema import CreateTable

import os
import sys

# PYTHONPATH=<base_project_dir> python file.py
import settings
from utils.db import dal, DataAccessLayer


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
            {"pixel": 9, "signal": 0.9, "ra": 100.9, "dec": 200.9},
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


def footprint_maps():
    map_table = Table('footprint_db', dal.metadata,
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
            {"pixel": 9, "signal": 0.9, "ra": 100.9, "dec": 200.9},
            {"pixel": 10, "signal": 1.0, "ra": 101.0, "dec": 201.0},
            {"pixel": 11, "signal": 1.1, "ra": 101.1, "dec": 201.1},
            {"pixel": 12, "signal": 2.2, "ra": 102.2, "dec": 201.2},
            {"pixel": 13, "signal": 2.3, "ra": 102.3, "dec": 201.3},
            {"pixel": 14, "signal": 2.4, "ra": 102.4, "dec": 201.4},
            {"pixel": 15, "signal": 2.5, "ra": 102.5, "dec": 201.5},
            {"pixel": 16, "signal": 2.6, "ra": 102.6, "dec": 201.6},
            {"pixel": 18, "signal": 2.8, "ra": 102.8, "dec": 201.8},
            {"pixel": 20, "signal": 2.0, "ra": 102.0, "dec": 201.10},
            {"pixel": 21, "signal": 2.1, "ra": 102.1, "dec": 201.11})

    print ("Creating footprint_map")
    _template_create(map_table, data)


def create_all_tables():
    systematic_maps()
    bad_regions()


def delete_all_tables():
    dal.metadata.drop_all(dal.engine)


if __name__ == '__main__':
    db = settings.DATABASES[settings.DATABASE]
    dal.db_init(DataAccessLayer.str_connection(db))

    print ("Creating systematic maps")
    systematic_maps()
    print ("Creating systematic maps 2")
    systematic_maps_2()
    print ("Creating bad region maps")
    bad_regions()
    print ("Creating footprint maps")
    footprint_maps()
