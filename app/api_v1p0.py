import json
import pprint
from flask import Blueprint, jsonify, request, g
from app import database
from app.exceptions import NotFoundException, NoJsonException
import happybase
import pysolr

blueprint = Blueprint(name='api_v1p0', import_name=__name__, url_prefix="/api/v1.0", template_folder='templates')

def decode_bytes(o):
    return o.decode('utf-8')

def validate_json(json):
    if not json or not hasattr(json, 'items'):
        raise NoJsonException()


@blueprint.route('/search/<string:search_string>', methods=['GET'])
def search(search_string):

    host	   = "edge2ai-1.dim.local"
    port	   = "8993"
    collection = "cdr-search"
    q          = "*:*"
    fl         = "UUID,Type,Response"
    qt         = "select"
    fq         = search_string
    rows       = "5"
    url        = 'http://' + host + ':' + port + '/solr/' + collection


    print(fq)

    connection = happybase.Connection('edge2ai-1.dim.local', transport='framed', protocol='compact', port=9091)
    table = connection.table('cdr')

    solr	   = pysolr.Solr(url, search_handler=""+qt, timeout=5)
    results    = solr.search(q, **{
        'fl': fl,
        'fq': fq,
        'rows': rows
    })

    response = []

    for i in results:
         row = table.row(i['UUID'][0])

         buffer = {}
         for k in row:
             buffer[k.decode("utf-8")] = row[k].decode("utf-8")

         response.append(buffer)

    return jsonify({"results": len(response), "response": response})


