from model import queries
import settings


def dfs_pre_order(node):
    if 'sub_op' in node:
        dfs_pre_order(node['sub_op'])
    if 'op' in node:
        print(node['description'])
    else:
        for sub_node in node.keys():
            dfs_pre_order(node[sub_node])
    return


if __name__ == "__main__":
    exp_time = queries.Operation('exposure_time')
    print(exp_time.query())
    print(exp_time.save_at())
    exp_time.create()

    bad_regions = queries.Operation('bad_regions')
    print(bad_regions.query())
    print(bad_regions.save_at())
    bad_regions.create()
