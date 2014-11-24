"""Hello World API implemented using Google Cloud Endpoints.

Defined here are the ProtoRPC messages needed to define Schemas for methods
as well as those methods defined in an API.
"""
import pickle
import json
import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

import MySQLdb
import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), "GAE/sqlengine/libs"))
sys.path.append(os.path.join(os.path.dirname(__file__), "GAE/sqlengine/libs/ks_graph/"))
sys.path.append(os.path.join(os.path.dirname(__file__), "modules/ks_merge/"))
sys.path.append(os.path.join(os.path.dirname(__file__), "modules/ks_analytics/"))
sys.path.append(os.path.join(os.path.dirname(__file__), "tests/ks_filehandler/"))
import networkx as nx
from ks_graph import generalLinksDB
from ks_merge import merge
from ks_merge import precompute
import ks_analytics
from ks_filehandler import filehandler

from user_analytics import measure_data

# TODO: Replace the following lines with client IDs obtained from the APIs
# Console or Cloud Console.
WEB_CLIENT_ID = 'replace this with your web client application ID'
ANDROID_CLIENT_ID = 'replace this with your Android client ID'
IOS_CLIENT_ID = 'replace this with your iOS client ID'
ANDROID_AUDIENCE = WEB_CLIENT_ID


package = 'Hello'

class MeasureDataInput(messages.Message):
    id = messages.IntegerField(1, required=True, default=3)
    frequency = messages.StringField(2, default="day")
    start_date = messages.StringField(3, required=True, default="2014-06-01")
    end_date = messages.StringField(4, required=True, default="2014-06-01")
    group_by = messages.StringField(5)
    dim_filter = messages.StringField(6)
    company_id = messages.StringField(7, default="1")

class Measure(messages.Message):
    name = messages.StringField(1)

class MeasureReturnItem(messages.Message):
    group_by = messages.StringField(1)
    value = messages.StringField(2)

class MeasureReturnItemList(messages.Message):
    return_list = messages.MessageField(MeasureReturnItem, 1, repeated=True)


class MeasureList(messages.Message):
    """Greeting that stores a message."""
    measures = messages.MessageField(Measure, 1, repeated=True)


@endpoints.api(name='ks_analytics', version='v1',
               allowed_client_ids=[WEB_CLIENT_ID, ANDROID_CLIENT_ID,
                                   IOS_CLIENT_ID],
               audiences=[ANDROID_AUDIENCE])
class HelloWorldApi(remote.Service):
    """ks_analytics v1"""

#*****************MEASURE LIST
    @endpoints.method(message_types.VoidMessage, MeasureList,
                      path='measuresGetAll', http_method='GET',
                      name='measures.getAll')
    def measure_list(self, unused_request):
        measure_list = []
        _INSTANCE_NAME = 'ks-sqlengine:test'
        
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='source', user='root')
        else:
            db = MySQLdb.connect(host='127.0.0.1', db='source', user='root', passwd='1193')
        
        
        ks_fh = filehandler(db)
        measures = ks_fh.getAllMeasures()
        db.close()
        
        for measure in measures:
            measure_list.append(Measure(name=str(measure)))
        
        return MeasureList(measures=measure_list)

#*****************MEASURE DATA
    @endpoints.method(MeasureDataInput, MeasureReturnItemList,
                      path='measuresGetData', http_method='GET',
                      name='measures.getData')
    def measure_data(self, request):
        measure_list = []
        _INSTANCE_NAME = 'ks-sqlengine:test'
        
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='source', user='root')
        else:
            db = MySQLdb.connect(host='127.0.0.1', db='source', user='root', passwd='1193')
        
        ks_fh = filehandler(db)
        
        company_id = 1
        measure_ids = []
        measure_ids.append(request.id)
        result = measure_data(db, request.company_id,  measure_ids, None, request.start_date, request.end_date,
                              request.group_by, None,request.dim_filter)
        
        for measure in result:
            item_list = []
            for key in result[measure]:
                item_list.append(MeasureReturnItem(group_by = key, value = str(result[measure][key])))
        result = MeasureReturnItemList(return_list = item_list)
        
               
        return result

    

APPLICATION = endpoints.api_server([HelloWorldApi])