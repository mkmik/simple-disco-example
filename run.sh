#!/bin/sh
cp data.txt out.txt

PASSES=10

for i in $(seq 1 $PASSES); do
    echo "pass $i"
    python ./test.py
done

echo "finished $PASSES passes"
cat out.txt