import settings
from utils.db import select_columns
from plots import histograms, surface
from science import sci

import collections
from multiprocessing import Process


class IEvent():
    def run(self, intermediate_table, params):
        pass


class Maps(IEvent):
    def run(self, intermediate_table, params):
        # plot surface 
        cols_suf = ['ra', 'dec', 'signal']
        data_suf = select_columns(intermediate_table.save_at(), cols_suf)

        p = []
        p.append(Process(target=surface.plot_map, args=[params, data_suf]))
        [x.start() for x in p]
        [x.join() for x in p]


class GreatEqual(Maps):
    def run(self, intermediate_table, params):
        Maps.run(self, intermediate_table, params)
        _data = select_columns(intermediate_table.save_at(), ['signal'])
        data = [s[0] for s in _data]

        p = []
        p.append(Process(target=histograms.plot, args=[params, data]))
        [x.start() for x in p]
        [x.join() for x in p]


class CombinedMaps(Maps):
    pass


class BadRegions(Maps):
    pass


class Footprint(Maps):
    pass


class Reduction(IEvent):
    pass


class ZeroPoint(IEvent):
    pass


class Cuts(IEvent):
    pass


class Bitmask(IEvent):
    pass


class Catalogs(IEvent):
    def run(self, intermediate_table, params):
        # histogram and surface data
        cols = ['ra', 'dec']
        radec = select_columns(intermediate_table.save_at(), cols)

        d = collections.defaultdict(int)
        for ra, dec in radec:
            pixel = sci.ang2pix(ra, dec, settings.G_PARAMS['nside'])
            d[pixel] += 1

        pix_area = sci.pixel_area(settings.G_PARAMS['nside'])
        values = [value / (3600 * pix_area) for value in d.values()]

        # Process to concurrent tasks.
        p = []
        p.append(Process(target=histograms.plot, args=[params, values]))
        p.append(Process(target=surface.plot_catalog, args=[params,
                         settings.G_PARAMS['nside'], radec]))
        [x.start() for x in p]
        [x.join() for x in p]


class ObjectSelection(Catalogs):
    pass


class SgSeparation(Catalogs):
    pass


class PhotoZ(Catalogs):
    pass


class GalaxyProperties(Catalogs):
    pass
