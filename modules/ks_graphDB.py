import networkx as nx
import matplotlib.pyplot as plt

class generalLinksDB:
    """
    Finds relations between tables using their headers.
    """
    def __init__(self, tables, merge):
        self.merge = merge
        nodes = []
        #add nodes
        for table in tables:
            nodes.extend(self.table2nodes(table))
               
            
        self.G=nx.Graph()
        #add edges
        for first_node in nodes:
            for second_node in nodes:
                [first_dim, first_level] = first_node.split(':')
                [second_dim, second_level] = second_node.split(':')
                if (first_dim == second_dim):
                    self.G.add_edge(first_node, second_node)
                if (first_dim != second_dim and first_level == second_level):
                    self.G.add_edge(first_node, second_node)
              
    def plot(self):                
        nx.draw_networkx(self.G)
        plt.show() # display
            
    def isEdge(self, node1, node2):
        for edge in self.G.edges_iter():
            first_node = edge[0]
            second_node = edge[1]
            if ((first_node == node1) & (second_node == node2)) or\
                 ((first_node == node2) & (second_node == node1)):
                return True
        return False         
            
            
    def getLinks(self):
        edges = []
        for edge in self.G.edges_iter():
            first_node = edge[0]
            second_node = edge[1]
            [first_dim, first_level] = first_node.split(':')
            [second_dim, second_level] = second_node.split(':')
            if (first_dim != second_dim):
                edges.append(edge)
        return edges
                
                
          
    def table2nodes(self, tablename):
        header = self.merge.getHeader(tablename)
        nodes = []
        for col in header:
            if not(col == "id"):
                nodes.append(tablename + ":" + col)
        return nodes    
                 

