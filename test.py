import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings
import simplejson as json

def map(line, params):
    def nextDistance(d):
        if d == -1:
            return -1
        return d + 1

    comps = json.loads(line)
    
    node = comps[0]
    distance = comps[1]
    
    nodes = comps[2]
    
    yield node, dict(id=1, nodes=nodes, distance=distance)

    for n in nodes:
        yield n, dict(id=n, distance=nextDistance(distance))

def reduce(iter, params):
    def mymin(a, b):
        return min([x for x in (a,b) if x != -1])

    from disco.util import kvgroup
    for node, distances in kvgroup(sorted(iter)):
        nodes = []
        distance = -1
        distances = list(distances)
        for d in distances:
            if d.get("nodes"):
                nodes = d["nodes"]
                distance = d["distance"]
                break

        for d in distances:
            if d["distance"] > 0:
                distance = mymin(distance, d["distance"])

        yield node, json.dumps([node,distance,nodes])

disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="shortestpath",
                        input=["file:///home/marko/tmp/disco/out.txt"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done"

out = file("out.txt", "w")

for node, data in result_iterator(results):
    print >>out, data

out.close()

