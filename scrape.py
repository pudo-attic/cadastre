from urlparse import urljoin
import requests
import dataset
from datetime import datetime


LAYERS_URL = 'MapServer/layers?f=json&pretty=true'
DATA_URL = 'MapServer/%s/query?f=json&where=1%%3D1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100'


def scrape_portal(base_url, name):
    e = dataset.connect('sqlite://')
    tbl = e['licenses']
    layers = requests.get(urljoin(base_url, LAYERS_URL)).json()
    for layer in layers.get('layers'):
        print name, layer.get('id'), layer.get('name')
        #if 'License' not in layer.get('name'):
        #    continue
        snip = DATA_URL % layer.get('id')
        data = requests.get(urljoin(base_url, snip)).json()
        
        for f in data.get('features'):
            a = f.get('attributes')
            o = {}
            for k, v in a.items():
                k = k.lower()
                if k.startswith('dte') and v is not None:
                    dt = datetime.fromtimestamp(int(v) / 1000)
                    v = dt.isoformat()
                o[k] = v
            o['LayerName'] = layer.get('name')
            o['LayerID'] = layer.get('id')
            #print a, name
            tbl.insert(o)

        #print data.get('features'), name

    dataset.freeze(tbl, filename="data/%s.csv" % name, format='csv')


PORTALS = {
    'kenya': 'http://166.78.48.49/ArcGIS/rest/services/KenyaMapPortal/',
    'zambia': 'http://166.78.48.49/ArcGIS/rest/services/ZambiaMapPortal/',
    'tanzania': 'http://166.78.48.49/ArcGIS/rest/services/TanzaniaFS/',
    'uganda': 'http://166.78.48.49/ArcGIS/rest/services/UgandaMapPortal/'
}

for name, url in PORTALS.items():
    scrape_portal(url, name)
