import cgi
import webapp2
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext import blobstore



import MySQLdb
import os
import sys


sys.path.append(os.path.join(os.path.dirname(__file__), "GAE/sqlengine/libs"))
import networkx as nx
sys.path.append(os.path.join(os.path.dirname(__file__), "GAE/sqlengine/libs/ks_graph/"))
from ks_graph import generalLinksDB
sys.path.append(os.path.join(os.path.dirname(__file__), "modules/ks_merge/"))
from ks_merge import merge
from ks_merge import precompute
sys.path.append(os.path.join(os.path.dirname(__file__), "modules/ks_analytics/"))
import ks_analytics
sys.path.append(os.path.join(os.path.dirname(__file__), "tests/ks_filehandler/"))
from ks_filehandler import filehandler

from register_raw_files import register_raw_files2table
from load_precompute_normalize import load_precompute_normalize_blob
from user_analytics import measure_data

class MainPage(webapp2.RequestHandler):
    def get(self):
        item_fh = """   <a href="%sfilehandler">Filehandler</a> <br>""" % self.request.url
        item_show = """   <a href="%sDisplayRawFiles">Show raw files</a> <br>""" % self.request.url
        item_merge = """   <a href="%sMergeDemo">Merge Demo</a> <br>""" % self.request.url
        item_api = """   <a href="%sAPIDemo">API Demo</a> <br>""" % self.request.url

        #self.response.write(" <a href='url'>link text</a> ")
        self.response.out.write("<html><body>")
        self.response.out.write("<p>SQL Engine</p>")
        self.response.out.write(item_fh)
        self.response.out.write(item_show)
        self.response.out.write(item_merge)
        self.response.out.write(item_api)
        self.response.out.write("</body></html>")
        


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads('file') # 'file' is file upload field in the form
        self.redirect("/filehandler")

class ShowRawFiles(webapp2.RequestHandler):
    def get(self):
        self.response.write("<p> Raw Files</p>")
        blobs = blobstore.BlobInfo.all()
        for blob in blobs:
            self.response.write(blob.filename + "," + str(blob.key()) + "<br>")

class MergeDemo(webapp2.RequestHandler):
    def get(self):
        _INSTANCE_NAME = 'ks-sqlengine:test'
        self.response.write("<p> Merge Demo </p>")
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='source', user='root')
        else:
            db = MySQLdb.connect(host='127.0.0.1', db='source', user='root', passwd='1193')
        ks_merge = merge(db)
        ks_merge.reset()
        ks_merge.addTableBlob("zuqPY_kcDIl6EIycRouGqw==","Sales")
        ks_merge.addTableBlob("1BFRDWzk7AYHVVI7KCVGDg==","Currencyv2")
        ks_merge.addTableBlob("WwtlZebfXtHdVj0u4xCBYQ==","CountryRegion")
        ks_merge.addTableBlob("Zzn6h8r4qFjxE13H-rKUow==","ComissionTax")
        ks_merge.automaticMerge()
        db.close()


class APIDemo(webapp2.RequestHandler):
    def get(self):
        _INSTANCE_NAME = 'ks-sqlengine:test'
        self.response.write("<p> API Demo </p>")
        if (os.getenv('SERVER_SOFTWARE') and
            os.getenv('SERVER_SOFTWARE').startswith('Google App Engine/')):
            db = MySQLdb.connect(unix_socket='/cloudsql/' + _INSTANCE_NAME, db='source', user='root')
        else:
            db = MySQLdb.connect(host='127.0.0.1', db='source', user='root', passwd='1193')
        
        # reset DB
        ks_fh = filehandler(db)
        ks_fh.reset()
        ks_merge = merge(db)
        ks_merge.reset()
        ks_precompute = precompute(db)
        ks_precompute.reset()
        
        company_id = "1"
        sales_table = "zuqPY_kcDIl6EIycRouGqw=="
        currency_table = "1BFRDWzk7AYHVVI7KCVGDg=="    
        country_table = "WwtlZebfXtHdVj0u4xCBYQ=="
        comission_table = "Zzn6h8r4qFjxE13H-rKUow=="    
        register_raw_files2table(sales_table, company_id, "Sales", db)
        register_raw_files2table(currency_table, company_id, "Currencyv2", db)
        register_raw_files2table(country_table, company_id, "CountryRegion", db)
        register_raw_files2table(comission_table, company_id, "ComissionTax", db)
        
        
        load_precompute_normalize_blob(company_id, db)
        data = measure_data(db, company_id, [3,4])
        self.response.write(data)
        db.close()


class UploadHTML(webapp2.RequestHandler):
    def get(self):
        upload_url = blobstore.create_upload_url('/upload')
        html_string = """
        <form action="%s" method="POST" enctype="multipart/form-data">
        Upload File:
        <input type="file" name="file"> <br>
        <input type="submit" name="submit" value="Submit">
        </form>""" % upload_url
        self.response.write(html_string)

application = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/filehandler', UploadHTML),
    ('/DisplayRawFiles', ShowRawFiles),
    ('/MergeDemo', MergeDemo),
    ('/APIDemo', APIDemo),
    ('/upload', UploadHandler),
], debug=True)

