import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings
import simplejson as json

def map(line, params):
    def nextDistance(d):
        if d == -1:
            return -1
        return d + 1

    def mapValues(f, d):
        return dict([(k, f(v)) for k, v in d.items()])

    comps = json.loads(line)
    
    node = comps[0]
    distances = comps[1]
    
    nodes = comps[2]
    
    yield node, dict(id=node, nodes=nodes, distances=distances)

    for n in nodes:
        yield n, dict(id=n, distances=mapValues(nextDistance, distances))

def reduce(iter, params):
    def mymin(a, b):
        mins = [x for x in (a,b) if x != -1]
        if not mins:
            return -1
        return min(mins)

    from disco.util import kvgroup
    for node, distances in kvgroup(sorted(iter)):
        nodes = []
        distances = list(distances)
        newdistances = {}

        def minFrom(d, a):
            for k, v in a.items():
                d[k] = mymin(d.get(k, -1), v)
        
        for d in distances:
            if d.get("nodes"):
                nodes = d["nodes"]
            minFrom(newdistances, d["distances"])

        yield node, json.dumps([node,newdistances,nodes])

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

