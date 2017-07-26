from sqlalchemy.sql import select
from sqlalchemy import Table

from utils.db import dal
from plots import histograms

from multiprocessing import Process


class IEvent():
    def run(self, intermediate_table, params):
        raise NotImplementedError("Implement this method")


class GreatEqual(IEvent):
    def run(self, intermediate_table, params):
        with dal.engine.connect() as con:
            table = Table(intermediate_table.save_at(), dal.metadata,
                          autoload=True, schema=dal.schema_output)
            sql = select([table.c.signal])
            res = con.execute(sql).fetchall()

            signal = [s[0] for s in res]

        # Process to concurrent tasks.
        p = Process(target=histograms.plot_map, args=[params, signal])
        p.start()
        p.join()


class CombinedMaps(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class BadRegions(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class Footprint(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class Reduction(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class ZeroPoint(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class Cuts(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class Bitmask(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class ObjectSelection(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class SgSeparation(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class PhotoZ(IEvent):
    def run(self, intermediate_table, params):
        print(params)


class GalaxyProperties(IEvent):
    def run(self, intermediate_table, params):
        print(params)
