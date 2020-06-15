import json
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent


def collect_dict(ss_obj):
    init_list = list(vars(ss_obj))[:-3]
    init_dict = dict(map(lambda k: (k, vars(ss_obj).get(k, None)), init_list))
    meta_dict = vars(ss_obj.metadata)
    anal_dict = vars(ss_obj.analysis)
    comp_dict = vars(ss_obj.components)
    temp = {**init_dict, **meta_dict, **anal_dict, **comp_dict}
    return json.dumps({str(k): str(v) for k, v in temp.items()})
