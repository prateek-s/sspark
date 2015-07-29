#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print >> sys.stderr, "Usage: wordcount <file>"
    #     exit(-1)
    sc = SparkContext(appName="PythonWordCount")
    d = range(1,100000)
    frac = 0.25 
    sd = sc.parallelize(d)

    while True:
        dm1 = sd.map(lambda x: x)
        dm2 = sd.map(lambda x: frac*x*x)
        dr1 = dm1.reduce(lambda x,y:x+y)
        dr2 = dm1.reduce(lambda x,y:x+y)
        print "------------ DECISION POINT-----------------"
        if dr1 > dr2 :
            print "dr1 is greater"
            sd.map(lambda x:x).collect()
            break
        else:
            print "dr2 is greater"
            frac = frac/10.0 
            sd.map(lambda x: frac*x*x).collect()

    # lines = sc.textFile(sys.argv[1], 1)
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #               .map(lambda x: (x, 1)) \
    #               .reduceByKey(add)
    # output = counts.collect()
    # for (word, count) in output:
    #     print "%s: %i" % (word, count)

    sc.stop()
