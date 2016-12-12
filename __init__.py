from model import queries
import settings


if __name__ == "__main__":
    exp_time = queries.Operation('exposure_time')
    print(exp_time.query())
    print(exp_time.save_at())
    exp_time.create()

    bad_regions = queries.Operation('bad_regions')
    print(bad_regions.query())
    print(bad_regions.save_at())
    bad_regions.create()
