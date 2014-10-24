import glob, json
from datetime import datetime
import dataset

LAYERS = {
    '0': 'Active EL Licenses',
    '1': 'Active ML Licenses',
    '2': 'Active LL Licenses',
    '3': 'Active RL Licenses',
    '4': 'Application EL Licenses',
    '5': 'Application ML Licenses',
    '6': 'Application LL Licenses',
    '7': 'Application RL Licenses'
}

e = dataset.connect('sqlite://')
tbl = e['licenses']

for id, name in LAYERS.items():
    with open('data/ug_%s.json' % id, 'r') as fh:
        data = json.load(fh)
        for f in data.get('features'):
            a = f.get('attributes')
            for k, v in a.items():
                if k.startswith('Dte') and v is not None:
                    dt = datetime.fromtimestamp(int(v)/1000)
                    a[k] = dt.isoformat()
            a['LayerName'] = name
            a['LayerID'] = id
            print a, name
            tbl.insert(a)

        #print data.get('features'), name

dataset.freeze(tbl, filename="licenses_ug.csv", format='csv')
