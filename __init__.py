from model import queries
import settings


if __name__ == "__main__":
    element = {'band': 'g', 'value': '0.55', 'name': 'exposure_time_i'}
    exp_time = queries.ExposureTime(element)
    print(exp_time.query())
    print(exp_time.save_at())
    exp_time.create()

    bad_regions = queries.BadRegions()
    print(bad_regions.query())
    print(bad_regions.save_at())
    bad_regions.create()
