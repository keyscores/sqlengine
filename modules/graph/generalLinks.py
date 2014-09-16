import matplotlib.pyplot as plt
import csv
import networkx as nx


class generalLinks:
    def __init__(self, csv_files):
        nodes = []
        #add nodes
        for csv_file in csv_files:
            nodes.extend(self.csv2nodes(csv_file))
        self.G=nx.Graph()
        #add edges
        for first_node in nodes:
            for second_node in nodes:
                [first_dim, first_level] = first_node.split('.')
                [second_dim, second_level] = second_node.split('.')
                if (first_dim == second_dim):
                    self.G.add_edge(first_node, second_node)
                if (first_dim != second_dim and first_level == second_level):
                    self.G.add_edge(first_node, second_node)
              
    def plot(self):                
        nx.draw_networkx(self.G)
        plt.show() # display
            
    def getLinks(self):
        for edge in self.G.edges_iter():
            first_node = edge[0]
            second_node = edge[1]
            [first_dim, first_level] = first_node.split('.')
            [second_dim, second_level] = second_node.split('.')
            if (first_dim != second_dim):
                print edge
                
    @staticmethod         
    def csv2nodes(filename):
        csv_reader = csv.reader(open(filename,"rb"))
        header = csv_reader.next()
        nodes = []
        for col in header:
            nodes.append(filename + col)
        return nodes    
                 

