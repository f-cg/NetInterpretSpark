from  itertools import chain


def compute_iau(row):
    record = row.index_line.split(',')
    layer_id = row.layer_id
    feature_map = row.feature_map
    cols = ['image','split','ih','iw','sh','sw','color','object','part','material','scene','texture']
    map_flat = list(chain(*feature_map))
    count = len(map_flat)
    for i, r in enumerate(record):
        if r=='':
            continue
        es = r.split(';')
        for i, e in enumerate(es):
            if e.isdigit():
                intersect = sum(x>shresh for x in map_flat)

    print()
    return row