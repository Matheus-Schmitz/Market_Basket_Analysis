'''
DSCI 553 | Foundations and Applications of Data Mining
Homework 2
Matheus Schmitz
USC ID: 5039286453
'''

# export PYSPARK_PYTHON=python3.6
# export PYSPARK_DRIVER_PYTHON=python3.6
# spark-submit task1.py 1 4 small1.csv results1a.csv
# spark-submit task1.py 2 9 small1.csv results1b.csv

import sys
from pyspark import SparkContext, SparkConf
from operator import add
import time
import math
import os

#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

def formattedCandidateCounts(partition):
	# Get a dicitionary with the count of all singletons in this partition
	conts_dict = countCandidates(partition)

	# Single item candidates become strings instead of tuple, fix that
	for candidate, count in conts_dict.items():
		candidate_as_tuple = candidate if type(candidate) == tuple else tuple([candidate])
		yield candidate_as_tuple, count


def countCandidates(partition):
	# Count the number of candidate occurances in each partition (which is a set of baskets)
	counts = {}
	# Loop though each basket in the partition
	for list_iterator in partition:
		# Loop through each set of cancidates from apriori (which are indexed by itemset size)
		for candidate in candidate_itemsets.value:
			# For each candidate itemset of a given size, check if all its sub-elements are in a given basket, if yes, then add 1 to the count
			for itemset in candidate[1]:
				# Coerce singletorns to tutple type
				itemset_as_tuple = itemset if type(itemset) == tuple else tuple([itemset])
				# If all items in the itemset are frequent in a given basket, increase the itemset's counter by 1
				if all(True if item in list_iterator else False for item in itemset_as_tuple):
					if counts.get(itemset) == None:
						counts[itemset] = 0
					counts[itemset] += 1
	return counts


def apriori(partition):
	# Start with singletons, aka itemset size of one, aka k=1
	k_size = 1

	# Get all frequent singles so that apriori can run its iterations
	freq_items, baskets = frequentSingles(partition)

	# Output frequent sets of size 1 (aka frequent singles)
	yield k_size, freq_items

	# Reshape the singles to (-1, 1) so that they are standardized to the shape of future itemsets
	freq_items = [set([single]) for single in freq_items]

	# Loop over an increasing k value until no frequent itemsets are found in this partition
	while k_size >= 1:
		k_size += 1

		# Generate candidate itemsets for this partition
		candidate_itemsets = {}
		for a in freq_items:
			for b in freq_items:
				if len(set(a).union(set(b))) == k_size:
					candidate_itemsets.update({tuple(sorted(set(a).union(set(b)))): 0})
		
		# Loop through candidate_itemsets if any were generated
		if bool(candidate_itemsets):
			# Check each candidate pair in each basket
			for itemset in candidate_itemsets.keys():
				for basket in baskets:
					# Count number of baskets that have all itemset elements as frequent items
					if all(True if item in basket else False for item in itemset):
						candidate_itemsets[itemset] += 1
			
			# Keep only the items which pass the weighted threshold
			freq_items = [itemset for itemset, count in candidate_itemsets.items() if count >= support.value/n_part.value]
			
			# If there are k-sized itemsets yield them and move to the next k
			if bool(freq_items):
				yield k_size, freq_items
			# If no frequent itemset for this value of k was found, then stop
			else:
				break
		# If no candidate sets were generating in this iteration, then step
		else:
			break


def frequentSingles(partition):
	singleton_counts, baskets  = {}, []

	# Count item occurances in each basket in this partition
	for list_of_values_grouped_by_key in partition:
		# Also append the basket to a list contining all baskets
		baskets.append(list_of_values_grouped_by_key)
		for i in list_of_values_grouped_by_key:
			if singleton_counts.get(i) == None:
				singleton_counts[i] = 0
			singleton_counts[i] += 1

	# Filter occurances to keep only frequent singletons (considering a weighted threshold based on the number of partitions)
	freq_singles = [item for item, count in singleton_counts.items() if count >= support.value/n_part.value]
	freq_singles.sort()

	return freq_singles, baskets


if __name__ == '__main__':
	start_time = time.time()

	case_number = sys.argv[1]
	support = int(sys.argv[2])
	input_file_path = sys.argv[3]
	output_file_path = sys.argv[4]

	# Initialize Spark with the 4 GB memory parameters from HW1
	sc = SparkContext.getOrCreate(SparkConf().set("spark.executor.memory", "4g").set("spark.driver.memory", "4g"))

	# Read the CSV skipping its header
	csvRDD = sc.textFile(input_file_path, min(support//2, 8))
	#csvRDD = sc.textFile(input_file_path, round(math.sqrt(support)))
	csvHeader =  csvRDD.first()
	csvRDD = csvRDD.filter(lambda row: row != csvHeader)

	# Shape RDD acording to case_number. Make sure to use a name which shouldn't conflict with basket namings elsewhere
	if case_number == '1':
		bskts = csvRDD.map(lambda row: (str(row.split(',')[0]), str(row.split(',')[1]))).groupByKey().map(lambda row: list(row[1]))
	elif case_number == '2':
		bskts = csvRDD.map(lambda row: (str(row.split(',')[1]), str(row.split(',')[0]))).groupByKey().map(lambda row: list(row[1]))

	# Make the number of partitions and the support threshold available to all nodes
	n_part= sc.broadcast(float(bskts.getNumPartitions()))
	support = sc.broadcast(float(support))

	# Apply A-Priori to each partition to find the candidate itemsets
	candidatesRDD = bskts.mapPartitions(lambda partition: apriori(partition)).reduceByKey(lambda x, y: sorted(set(x + y))).sortBy(lambda candidate: candidate[0]).collect()
	# The ta_feng_all_months_merged.csv dataset is rather small (62MB) so swapping Spark's sort for Python's sort could improve performance a good deal
	#candidatesRDD = bskts.mapPartitions(lambda partition: apriori(partition)).reduceByKey(lambda x, y: sorted(set(x + y))).collect()
	#candidatesRDD.sort()
	candidate_itemsets = sc.broadcast(candidatesRDD)

	# Write the candidate itemsets to the output file
	# Turns out that f-string format is much more performance efficient than the alternatives (if it gives a Spark error, then need to export PYSPARK_PYTHON=python3.6)
	with open(output_file_path, 'w') as fout:
		# Write the header for the candidates step
		fout.write('Candidates:')
		# Use a special output for the singletons, since they are not stored as tuples by python
		fout.write('\n' + ','.join([f"('{candidate}')" for candidate in candidate_itemsets.value[0][1]]))
		# For all other k-sized itemsets simply output them
		for candidates in candidate_itemsets.value[1:]:
			fout.write('\n\n' + ','.join([f"{candidate}" for candidate in candidates[1]]))

	# Count candidates to see which are truly frequent itemsets
	freqItemsetsRDD = bskts.mapPartitions(lambda partition: formattedCandidateCounts(partition)).reduceByKey(add).filter(lambda candidate: candidate[1] >= support.value).keys().sortBy(lambda itemset: (len(itemset), itemset)).collect()
	# The ta_feng_all_months_merged.csv dataset is rather small (62MB) so swapping Spark's sort for Python's sort could improve performance a good deal
	#freqItemsetsRDD = bskts.mapPartitions(lambda partition: formattedCandidateCounts(partition)).reduceByKey(add).filter(lambda candidate: candidate[1] >= support.value).keys().collect()
	#freqItemsetsRDD.sort(key=(lambda itemset: (len(itemset), itemset)))

	# Organize the itemsets by size, for outputting
	output = dict()
	for itemset in freqItemsetsRDD:
		itemset_size = len(itemset)
		if output.get(itemset_size) == None:
			output[itemset_size] = []
		output[itemset_size].append(itemset)

	# Open the output file in append mode and add the frequent itemsets
	# Turns out that f-string format is much more performance efficient than the alternatives (if it gives a Spark error, then need to export PYSPARK_PYTHON=python3.6)
	with open(output_file_path, 'a') as fout:
		# Add a header for Frequent Itemsets
		fout.write('\n\n' + 'Frequent Itemsets:')
		# Use a special output for the singletons, since they are not stored as tuples by python
		fout.write('\n' + ','.join([f"('{itemset[0]}')" for itemset in output[1]]))
		# For all other k-sized itemsets simply output them
		for itemset_size in list(output.keys())[1:]:
			fout.write('\n\n' + ','.join([f"{itemset}" for itemset in output[itemset_size]]))

	# Measure the total time taken and report it
	time_elapsed = time.time() - start_time
	print(f'Duration: {time_elapsed}')