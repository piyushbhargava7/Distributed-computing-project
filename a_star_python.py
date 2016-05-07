import pandas  as pd
import numpy as np
from math import radians, sin, cos, sqrt, asin


distances = pd.read_csv("USA-road-d.BAY.gr", header = None, sep = ' ')
distances.columns = ['ind', 'from_node', 'to_node', 'dist']
distances['from_node'] = distances['from_node'].apply(str)
distances['to_node'] = distances['to_node'].apply(str)
distances['dist'] = distances['dist'].map(lambda x: 1.0*x/10)
distances = distances.drop('ind', axis = 1)

time = pd.read_csv("USA-road-t.BAY.gr", header = None, sep = ' ')
time.columns = ['ind', 'from_node', 'to_node', 'time' ]
time['from_node'] = time['from_node'].apply(str)
time['to_node'] = time['to_node'].apply(str)
time = time.drop('ind', axis = 1)

coordinates = pd.read_csv("USA-road-d.BAY.co", header = None, sep = ' ')
coordinates.columns = ['ind', 'node', 'long', 'lat' ]
coordinates['node'] = coordinates['node'].apply(str)
coordinates['long'] = coordinates['long'].map(lambda x: 1.0*x/1000000)
coordinates['lat'] = coordinates['lat'].map(lambda x: 1.0*x/1000000)
coordinates = coordinates.drop('ind', axis = 1)

def a_star(df, s, e):
    closedSet = set()
    openSet = set({s})
    cameFrom = {}
    gScore = {}
    gScore[s] = 0
    fScore = {}
    fScore[s] = 0
    while len(openSet) != 0:
        temp = {fScore[node]:node for node in openSet}
        current = temp[np.min(temp.keys())]
        if current == e:

            return reconstruct_path(cameFrom, current)
        openSet.remove(current)
        closedSet.add(current)
        for neighbor in list(df[df.from_node == current].to_node):
            if neighbor in closedSet:
                continue
            tentative_gScore = gScore[current] + df[df.from_node==current][df.to_node== neighbor].dist.values[0]
            if neighbor not in openSet:	# Discover a new node
                openSet.add(neighbor)
            elif tentative_gScore >= gScore[neighbor]:
                continue		# This is not a better path.
            cameFrom[neighbor] = current
            gScore[neighbor] = tentative_gScore
            fScore[neighbor] = gScore[neighbor] + heuristic_cost_estimate(neighbor, e)
    return "Failed to find the end point"


def haversine((lat1, lon1), (lat2, lon2)):

  R = 6372.8 *1000 # Earth radius in meters
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)
  lat1 = radians(lat1)
  lat2 = radians(lat2)

  a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
  c = 2*asin(sqrt(a))
  return R * c


def heuristic_cost_estimate(neighbor, e):
    neighbor_c = coordinates[coordinates.node==neighbor]
    e_c = coordinates[coordinates.node==e]
    return haversine((neighbor_c.lat.values[0], neighbor_c.long.values[0]),
                     (e_c.lat.values[0], e_c.long.values[0]))


def reconstruct_path(cameFrom, current):
    total_path = [current]
    while current in cameFrom.keys():
        current = cameFrom[current]
        total_path.append(current)
    return total_path


s = '1'
e = '4'

import time
start_time = time.time()
path = a_star(distances, s, e)
print "Time taken", (time.time() - start_time)/60, " mins"

path_dist = 0
for i in range(len(path)-1):
    f = path[i]
    t = path[i+1]
    path_dist+=distances[distances.from_node == f][distances.to_node == t].dist.values[0]

print path
print path_dist