input
=====

The input file is an adjacency list with a distance from the pivot. The pivot is the node which has distance 0.

    [1,0,[2,3]]
    [2,-1,[3]]
    [3,-1,[4,5]]
    [4,-1,[]]
    [5,-1,[6]]
    [6,-1,[]]


'-1' means infinity distance. The algorithm, after some iterations will produce an output file with the computed distances from the pivot in the second position of the JSON array.

run
===


./run.sh

    pass 1
    Starting Disco job..
    Go to http://ributtalo:8989 to see status of the job.
    Job done
    pass 2
    Starting Disco job..
    Go to http://ributtalo:8989 to see status of the job.
    Job done
    pass 3
    .....
    Starting Disco job..
    Go to http://ributtalo:8989 to see status of the job.
    Job done
    pass 10
    Starting Disco job..
    Go to http://ributtalo:8989 to see status of the job.
    Job done
    finished 10 passes
    [1, 0, [2, 3]]
    [2, 1, [3]]
    [3, 1, [4, 5]]
    [4, 2, []]
    [5, 2, [6]]
    [6, 3, []]
