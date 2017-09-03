from pyspark import SparkContext
from collections import defaultdict
import timeit

sc = SparkContext("local", "WCo3Final")
logFile = "/home/hadoop/spark/README.md"  # Should be some file on your system
latinFile = sc.textFile("/home/hadoop/Downloads/new_lemmatizer.csv").collect()
files = sc.textFile("/home/hadoop/Downloads/files/*")

start_time = timeit.default_timer()
lemmaMap = {};
d = defaultdict(list)

for i in range(len(latinFile)):
    tokens = latinFile[i].split(",");
    t =  [x for x in tokens if x != '']
    #print("t!!! " , t)
    #print("t[0]!!! " , t[0].encode('ascii','ignore'))
    #print("t[1]!!! " , t[1].encode('ascii','ignore'))
    lemmaMap[t[0].encode('ascii','ignore')]=t[1].encode('ascii','ignore')

    for j in range (1,len(t)):
	d[t[0].encode('ascii','ignore')].append(t[j].encode('ascii','ignore'))

def Mapper(line):
	line.encode('ascii','ignore')
    	if(len(line)>1):
        	loc = line.split(">")[0].encode('ascii','ignore')
		tokens = (line.encode('ascii','ignore').split(">")[1])
        	tokens = tokens.replace("j","i")
        	tokens = tokens.replace("v","u")
        	tokens = tokens.replace("[^a-zA-Z]","")
        	tokens = tokens.replace("\t","")
        	tokens = tokens.lower()
		words=tokens.split(' ')


		wordco3 =[]
            	for i in range(len(words)-1):
    			for j in range(i+1,len(words)):
           			for k in range(i+2, len(words)):
                			if(words[i] in d.values() or words[j] in d.values() or words[k] in d.values()): 
                            			for t1 in d.get(words[i]):
                                			for t2 in d.get(words[j]):
                                    				for t3 in lemmaMap.get(words[k]):
                                          				wordco3.append((t1+","+t2  + ","+ t3,str(loc)+">"))
                                    
                			else:
                        			wordco3.append((words[i]+"," + words[j]  + "," + words[k],str(loc)+">"))


	return wordco3

def Reducer(word1,word2):
    return word1+","+ word2

finalOutput=files.flatMap(Mapper).reduceByKey(Reducer)
#finalOutput.saveAsTextFile('/home/hadoop/Desktop/DICLab5/wCo3_1.txt')
end_time = timeit.default_timer()
print(end_time - start_time)
sc.stop()

