from model import events as evt


class EventBuilder():
    ops = {}
    ops['great_equal'] = evt.GreatEqual()
    ops['join'] = evt.CombinedMaps()
    ops['bad_regions'] = evt.BadRegions()
    ops['footprint'] = evt.Footprint()
    ops['reduction'] = evt.Reduction()
    ops['cuts'] = evt.Cuts()
    ops['bitmask'] = evt.Bitmask()
    ops['zero_point'] = evt.ZeroPoint()
    ops['object_selection'] = evt.ObjectSelection()
    ops['sg_separation'] = evt.SgSeparation()
    ops['photoz'] = evt.PhotoZ()
    ops['galaxy_properties'] = evt.GalaxyProperties()

    @staticmethod
    def create(operation_type):
        try:
            operation = EventBuilder.ops[operation_type]
            return operation
        except:
            raise "The %s operation is not implemented." % operation_type
