import webapp2
import MySQLdb
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import networkx as nx
sys.path.append(os.path.join(os.path.dirname(__file__), "libs/ks_graph/"))
from ks_graph import generalLinksDB
sys.path.append(os.path.join(os.path.dirname(__file__), "libs/ks_merge/"))
from ks_merge import merge
sys.path.append(os.path.join(os.path.dirname(__file__), "libs/ks_analytics/"))
import ks_analytics


class MainPage(webapp2.RequestHandler):
    def get(self):
       
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('Hello, World!')
        db = MySQLdb.connect(host='127.0.0.1', db='source', user='root', passwd='???')
        self.response.write(db)
	    G = nx.Graph()
        G.add_edge("A","B")
	    G.add_edge("C","D")
	    self.response.write("Graph:")
        ks_merge = merge(db)
        general_links = generalLinksDB(["Sales","CountryRegion"], ks_merge)
        self.response.write(G.number_of_edges())
        self.response.write(general_links.getLinks())


application = webapp2.WSGIApplication([
    ('/', MainPage),
], debug=True)

