import glob, json
from datetime import datetime
import dataset

countries = {
    'png': 'Papua New Guinea',
    'drc': 'DR Congo',
    'moz': 'Mozambique'
}

e = dataset.connect('sqlite://')
tbl = e['licenses']

for fn in glob.glob('data/*_all.json'):
    with open(fn, 'r') as fh:
        data = json.load(fh)
        #print fn, data
        for l in data.get('layers'):
            print l.keys()
            for f in l.get('features'):
                a = f.get('attributes')
                o = {}
                for k, v in a.items():
                    k = k.lower()
                    if k.startswith('dte')and v is not None:
                        dt = datetime.fromtimestamp(int(v) / 1000)
                        v = dt.isoformat()
                    o[k] = v
                tbl.insert(o)

    dataset.freeze(tbl, filename="%s.csv" % fn, format='csv')
