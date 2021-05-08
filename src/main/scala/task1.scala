// DSCI 553 | Foundations and Applications of Data Mining
// Homework 2
// Matheus Schmitz
// USC ID: 5039286453

// spark-submit --class task1 hw2.jar 1 4 small1.csv scala1a.csv
// spark-submit --class task1 hw2.jar 2 9 small1.csv scala1b.csv

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import scala.util.control.Breaks._
import scala.math
import java.io._
import Ordering.Implicits._


object task1 {

  def apriori(partition: Iterator[List[String]], support: Float, n_part: Float): Iterator[(Int, mutable.Set[Set[String]])] = {
    // Start with singletons, aka itemset size of one, aka k=1
    var k_size = 1
    var itemset_counts = mutable.Map.empty[Set[String], Int]
    var baskets = mutable.MutableList.empty[Set[String]]
    val candidate_itemsets_dict = mutable.Map.empty[Int, mutable.Set[Set[String]]]
    // Initialize the candidate_itemsets_dict at the current k_size so that its possible to later append candidates to it
    candidate_itemsets_dict(k_size) = mutable.Set.empty[Set[String]]

    // Count singleton occurrences in each basket in this partition
    for (list_of_values_grouped_by_key <- partition) {
      // # Also append the basket to a list containing all baskets
      baskets += list_of_values_grouped_by_key.toSet
      for (i <- list_of_values_grouped_by_key) {
        // In the speacial case of singletons, the strings need to first be converted to type Set
        if (!itemset_counts.contains(Set(i))) {
          itemset_counts += (Set(i) -> 0)}
        itemset_counts(Set(i)) += 1}}

    // For each itemset found, check if it passes the weighted threshold
    for (item <- itemset_counts) {
      if (item._2 >= support / n_part) {
        // If yes, add the itemset to the dict of candidates at the current k_size
        candidate_itemsets_dict(k_size) += item._1}}

    // Loop over an increasing k value until no frequent itemsets are found in this partition
    k_size += 1
    // Stop the loop if no candidates were generated for the previous k_value
    while (candidate_itemsets_dict(k_size - 1).size > 0) {

      // Initialize the candidate_itemsets_dict at the current k_size so that its possible to later append candidates to it
      candidate_itemsets_dict(k_size) = mutable.Set.empty[Set[String]]
      // Generate candidate itemsets for this partition
      var iteration_candidates = mutable.Set.empty[Set[String]]
      // Create a dictionary to store occurrence counts for itemsets at the current k_size
      itemset_counts = mutable.Map.empty[Set[String], Int]

      // Take all candidates from the previous k_size
      for (i <- candidate_itemsets_dict(k_size - 1)) {
        for (j <- candidate_itemsets_dict(k_size - 1)) {
          // And construct all possible combinations which match the current k_size
          val candidate = i.union(j).toList.sorted.toSet
          if (candidate.size == k_size) {
            // Then populate the set of new possible candidates
            iteration_candidates += candidate}}}

      // Loop through iteration_candidates if any were generated
      if (iteration_candidates.size > 0) {
        for (cand_itemset <- iteration_candidates) {
          // Compare each candidate with each basket
          for (basket <- baskets) {
            // And if the candidate is present in the basket, add 1 to its occurrence count
            if (cand_itemset.subsetOf(basket)) {
              if (!itemset_counts.contains(cand_itemset))
                itemset_counts += (cand_itemset -> 0)
              itemset_counts(cand_itemset) += 1}}}}

      // For each itemset found, check if it passes the weighted threshold
      for (candidate <- itemset_counts) {
        if (candidate._2 >= support / n_part) {
          // If yes, add the itemset to the dict of candidates at the current k_size
          candidate_itemsets_dict(k_size) += candidate._1}}

      // Go to the next k value in the loop
      k_size += 1}

    // Convert the dictionary to an iterator, as required by scala's Spark
    val candidate_itemsets_iterator = candidate_itemsets_dict.toIterator

    // Return the candidate itemsets for this partition
    return candidate_itemsets_iterator: Iterator[(Int, mutable.Set[Set[String]])]
  }

  def countCandidates(partition: Iterator[List[String]], candidatesRDD: Array[(Int, scala.collection.mutable.Set[Set[String]])]): Iterator[(Set[String], Int)] ={
    var conts_dict = mutable.Map.empty[Set[String], Int]
    for (basket <- partition) {
      for (k_sized_itemsets <- candidatesRDD)
        for (candidate_itemset <- k_sized_itemsets._2)
          if (candidate_itemset.subsetOf(basket.toSet)) {
            if (!conts_dict.contains(candidate_itemset))
              conts_dict += (candidate_itemset -> 0)
            conts_dict(candidate_itemset) += 1}}

    // Convert the dictionary to an iterator, as required by scala's Spark
    val conts_dict_iterator = conts_dict.toIterator

    // Return the itemsets counts for this partition
    return conts_dict_iterator: Iterator[(Set[String], Int)]
  }

  def broadcast_vars(parts: Float, supp: Float): (Broadcast[Float], Broadcast[Float]) = {
    val config = new SparkConf().setMaster("local[*]").setAppName("broadcaster").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")//.set("spark.testing.memory", "471859200")
    val sc = SparkContext.getOrCreate(config)

    val support = sc.broadcast(supp)
    val n_part = sc.broadcast(parts)

    return (n_part, support): (Broadcast[Float], Broadcast[Float])
  }

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()

    val case_number = args(0)
    val supp = args(1).toInt
    val input_file_path = args(2)
    val output_file_path = args(3)
    //val case_number = "1"
    //val supp = 4
    //val input_file_path = "small1.csv"
    //val output_file_path = "scala1a.csv"
    val txtoutputter = new PrintWriter(new File(output_file_path))
    
    // Initialize Spark with the 4 GB memory parameters from HW1
    val config = new SparkConf().setMaster("local[*]").setAppName("task1").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")//.set("spark.testing.memory", "471859200")
    val sc = SparkContext.getOrCreate(config)
    
    // Read the CSV skipping its header
    val csvRDD1 = sc.textFile(input_file_path, Set(supp/2, 8).min)
    //val csvRDD1 = sc.textFile(input_file_path, math.round(math.sqrt(supp)).toInt)
    val csvHeader = csvRDD1.first()
    val csvRDD2 = csvRDD1.filter(row => row != csvHeader)

    // Make sure to use a name which shouldn't conflict with basket namings elsewhere
    var bskts: RDD[List[String]] = sc.emptyRDD
    // Shape RDD according to case number
    if (case_number == "1") {
      bskts = csvRDD2.map(row => (row.split(',')(0), row.split(',')(1))).groupByKey().map(row => row._2.toList): RDD[List[String]]}
    else if (case_number == "2") {
      bskts = csvRDD2.map(row => (row.split(',')(1), row.split(',')(0))).groupByKey().map(row => row._2.toList): RDD[List[String]]}

    //Make the number of partitions and the support threshold available to all nodes
    //val (n_part, support) = broadcast_vars(bskts.getNumPartitions, supp.toFloat)
    val n_part = sc.broadcast(bskts.getNumPartitions.toFloat)
    val support = sc.broadcast(supp.toFloat)

    // Apply A-Priori to each partition to find the candidate itemsets
    val candidatesRDD = bskts.mapPartitions(partition => apriori(partition, support.value, n_part.value)).reduceByKey((x1, x2) => x1.union(x2)).sortBy(candidate => candidate._1).collect()

    // Track changes to k-value to know when to add newlines
    var current_row = 0
    // Write the candidate itemsets header to the output file
    txtoutputter.write("Candidates:")
    // Loop through the set of candidates
    for (k <- candidatesRDD) {
      // Sort the candidates lexicographically
      var sorted_k = k._2.toSeq.sortBy(candidate => candidate.toSeq)
      // Take one itemset at a time
      for (itemset <- sorted_k) {
        // When k increases update the current_row tracker and add newlines as needed
        if (current_row != k._1) {
          current_row = k._1
          // All rows are separated by two newlines, except the first row, which has only one
          if (current_row != 1) {
            txtoutputter.write("\n")}
          txtoutputter.write("\n")}
        // If itemset size didn't change, just separate them itemset with a comma
        else {
          txtoutputter.write(",")}

        // Proceed to writing the elements of the current itemset
        txtoutputter.write("(")
        // For each element in the itemset
        for (item <- itemset) {
          // Get position of the singleton in the itemset
          var current_item = 1 + itemset.toList.indexOf(item)
          // Write a value between quotes
          txtoutputter.write("'"+item+"'")
          // Then if current value is not the last one add a coma
          if (current_item < itemset.size) { //&& itemset.size != 1
            txtoutputter.write(", ")}
          // Or if it is the last value, then skip the comma and close parenthesis
          else {
            txtoutputter.write(")")}}}}

    // Count candidates to see which are truly frequent itemsets
    val freqItemsetsRDD = bskts.mapPartitions(partition => countCandidates(partition, candidatesRDD)).reduceByKey(_ + _).filter(itemset_counts => itemset_counts._2 >= support.value).sortBy(itemset => (itemset._1.size, itemset._1.toList)).collect()

    // Open the output file in append mode and add the frequent itemsets header
    txtoutputter.write("\n\n" + "Frequent Itemsets:")
    // Track changes to k-value to know when to add newlines
    current_row = 0
    // Loop through the set of candidates
    for (itemset_tuple <- freqItemsetsRDD) {
      var itemset = itemset_tuple._1
      // And update the current_k if needed
      if (current_row != itemset.size) {
        current_row = itemset.size
        // All rows are separated by two newlines, except the first row, which has only one
        if (current_row != 1) {
          txtoutputter.write("\n")}
        txtoutputter.write("\n")}
      // If itemset size didn't change, just separate them itemsets with a comma
      else {
        txtoutputter.write(",")}

      // Proceed to writing the elements of the current itemset
      txtoutputter.write("(")
      // For each element in the itemset
      for (item <- itemset) {
        // Get position of the singleton in the itemset
        var current_item = 1 + itemset.toList.indexOf(item)
          // Write a value between quotes
          txtoutputter.write("'"+item+"'")
          // Then if current value is not the last one add a coma
          if (current_item < itemset.size) { //&& itemset.size != 1
            txtoutputter.write(", ")}
          // Or if it is the last value, then skip the comma and close parenthesis
          else {
            txtoutputter.write(")")}}}

    // Close the PrinterWriter, saving the outputted lines
    txtoutputter.close()

    // Close Spark
    sc.stop()

    // Measure the total time taken and report it
    val total_time = System.currentTimeMillis() - start_time
    val time_elapsed = (total_time).toFloat / 1000.toFloat
    println("Duration: " + time_elapsed)
  }
}
