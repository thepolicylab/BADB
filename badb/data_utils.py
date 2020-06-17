import json
from pathlib import Path


ROOT_DIR = Path(__file__).parent.parent

def collect_dict(ss_obj):
<<<<<<< HEAD
  unwanted_keys = ['components', 'metadata', 'analysis']
  init_list = [e for e in list(vars(ss_obj)) if e not in unwanted_keys]
  init_dict = dict(map(lambda k: (k, vars(ss_obj).get(k, None)), init_list))
  temp = {**init_dict, **vars(ss_obj.metadata), **vars(ss_obj.analysis), **vars(ss_obj.components)}
  return json.dumps({str(k): str(v) for k,v in temp.items()})


=======
    unwanted_keys = ['components', 'metadata', 'analysis']
    init_list = [e for e in list(vars(ss_obj)) if e not in unwanted_keys]
    init_dict = dict(map(lambda k: (k, vars(ss_obj).get(k, None)), init_list))
    temp = {**init_dict, **vars(ss_obj.metadata), **vars(ss_obj.analysis), **vars(ss_obj.components)}
    return json.dumps({str(k): str(v) for k, v in temp.items()})
>>>>>>> kevin-changes
