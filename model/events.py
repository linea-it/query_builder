from sqlalchemy.sql import select
from sqlalchemy import Table

import settings
from utils.db import dal, select_columns
from plots import histograms, surface
from science import sci

import collections
from multiprocessing import Process


class IEvent():
    def run(self, intermediate_table, params):
        raise NotImplementedError("Implement this method")


class GreatEqual(IEvent):
    def run(self, intermediate_table, params):
        data = select_columns(intermediate_table.save_at(), ['signal'])
        cols_hist = ['ra', 'dec', 'signal']
        data_hist = select_columns(intermediate_table.save_at(), cols_hist)

        p = []
        p.append(Process(target=histograms.plot, args=[params, [s[0] for s in data]]))
        # p.append(Process(target=surface.plot_map, args=[params, data_hist]))
        [x.start() for x in p]
        [x.join() for x in p]


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


class Catalogs(IEvent):
    def run(self, intermediate_table, params):
        cols_hist = ['ra', 'dec']
        radec = select_columns(intermediate_table.save_at(), cols_hist)

        d = collections.defaultdict(int)
        for ra, dec in radec:
            pixel = sci.ang2pix(ra, dec, settings.G_PARAMS['nside'])
            d[pixel] += 1

        pix_area = sci.pixel_area(settings.G_PARAMS['nside'])
        values = [value / (3600 * pix_area) for value in d.values()]

        # Process to concurrent tasks.
        p = Process(target=histograms.plot, args=[params, values])
        p.start()
        p.join()


class ObjectSelection(Catalogs):
    pass


class SgSeparation(Catalogs):
    pass


class PhotoZ(Catalogs):
    pass


class GalaxyProperties(Catalogs):
    pass
