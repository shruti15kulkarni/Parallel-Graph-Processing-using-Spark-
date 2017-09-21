val fileRdd = sc.textFile("latin.tess")

val splitRdd = fileRdd.map ( line => line.split("\t") )

val nextStream = splitRdd.flatMap( locationAndLine => {
	val location:String = locationAndLine(0)
	val line:String = locationAndLine(1)
	
	val words = line.split(" ")
	
	val wordCombinations = words.combinations(2).toList.map(bigram => (bigram.mkString(","),location))
	
	wordCombinations
} )
nextStream.sortByKey().reduceByKey(_+_).foreach(println)
nextStream.sortByKey().reduceByKey(_+_).saveAsTextFile("result")