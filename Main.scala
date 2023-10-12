//Anas Khraim | 11924105
// assignment 1 bigData
import org.apache.log4j.BasicConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.varia.NullAppender

object Main {
  def main(args: Array[String]): Unit = {
//Constants--------------------------
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val conf = new SparkConf()
      .setAppName("Read CSV File")
      .setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sc = sparkContext

    //Defining TextFile
    val RDD = sc.textFile("src/main/resources/elonmusk_tweets.csv").filter(line => !line.trim.startsWith("id"))

    //Req 1 : the distribution of keywords over time (day-wise) the number of times each keyword is mentioned every day :
   print("Enter A List Of Words Separated By Commas{,} : ")
    val MyWord = scala.io.StdIn.readLine()
    // spliting input into a list of words by comma and trims any leading or trailing white spaces from each word
    val WordList = MyWord.split(",").map(_.trim).toList
  //Unifying the letters of words with uppercase or lowercase letters to make it easier to find the word
    val FilterdRDD = RDD.filter(Row => WordList.exists(word => Row.toUpperCase.contains(word.toUpperCase)))
    val DistributedKeywords = FilterdRDD
      .map(Row => Row.split(',')(1).substring(0, 10))
      .map(Date => (Date, 1))
      .reduceByKey(_ + _)
    println(" The distribution of keywords over time the number of times each keyword is mentioned every day : ")
    WordList.foreach { Word => var list = DistributedKeywords.collect()
      .map { case (date, counter) =>s" { $Word | $date | $counter }" }
      list.foreach(println)}


    //Req 2 :  the percentage of tweets that have at least one of these input keywords.
    val TotalNumOfTweets = RDD.count()
    val NumberOfWords = FilterdRDD.count()
    val res1 = NumberOfWords / TotalNumOfTweets
    val percent = s"${res1 * 100}%"
    print(" \n The percentage of tweets that have at least one of these input keywords = " + percent )

    // Req 3 :  the percentage of tweets that have exactly two input keywords.
    val ExactlyTwoWords = RDD.filter(Row =>WordList.count(word => Row.toUpperCase.contains(word.toUpperCase)) == 2 )
    val NoExactlyTwoWords = ExactlyTwoWords.count()
    val res2 = NoExactlyTwoWords / TotalNumOfTweets
    val percentOfExactlyTwoWords =s"${res2 * 100}%"
    print(" \n The percentage of tweets that have exactly two input keywords : " + percentOfExactlyTwoWords)



    //Req 4 :  the average and standard deviation of the length of tweets.

    //1st calculating avg ...
    print("\n \n the average and standard deviation of the length of tweets ")
    val Length = FilterdRDD.map { Row =>
      val text = Row.split(",")(2)
      val length = text.length
      (text, length)}
    val avg = (Length.map(_._2).reduce(_+_)) / NumberOfWords
    print(s"\n Average            :  $avg" )
    val standardDeviation = math.sqrt(
      FilterdRDD
        .map(Row => Row.split(",")(2).length)
        .map(value => math.pow(value - avg, 2))
        .sum / NumberOfWords).doubleValue()
    println(s"\n Standard Deviation :  $standardDeviation")

  }

}