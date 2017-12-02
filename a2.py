# Sabareesh
# Meenakshisundaram Balasubramanian
# smeenaks

from pyspark import SparkContext
import sys
sc = SparkContext()

def vertex(line):
    v = [ int(x) for x in line.split(" ") ]
    return [(v[0], v[1]), (v[1], v[0])]

# Converting the final output to required format
def saveFile(gamma):
    a = gamma[0]
    b = gamma[1]
    return str(a)+' '+str(b)

# Small Star algorithm
def smallStar(onegamma):
    N = [item for item in onegamma[1] if item < onegamma[0]]
    if not N:
        return []
    m = min (min (onegamma[1]), onegamma[0])
    return list(set([(v,m) if v!=m else (onegamma[0],v) for v in N]))

# Large Star algorithm
def largeStar(onegamma):
    N = [item for item in onegamma[1] if item > onegamma[0]]
    if not N:
        return []
    m = min (min (onegamma[1]), onegamma[0])
    return list(set([(v,m) if v!=m else (onegamma[0],v) for v in N]))

# Driver code
gamma = sc.textFile(sys.argv[1]).flatMap(vertex)   
while(True):
    gamma1 = gamma.groupByKey().flatMap(largeStar)
    if not gamma1.count():                      # checking for convergence
        g = gamma                               # result variable
        break
    gamma = gamma1.groupByKey().flatMap(smallStar)
    if not gamma.count():                       # checking for convergence
        g = gamma1                              # result variable
        break

numPart = g.getNumPartitions()
k = g.groupBy(lambda tup: (tup[1])).map(lambda x: (x[0], x[0]))
g = g.union(k).coalesce(numPart)        # low cost
g = g.map(saveFile)
g.saveAsTextFile("out")                 # Write output

sc.stop() # Done