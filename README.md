# BD_SearchEngine

Simple Search Engine in Spark. https://hackmd.io/MK6b5hQgQm6j-4KwVWZuQA?view

indexer.scala  -  read JSON files, count words, calculate TF, IDF, unify tables, write to JSON files
indexer class: args(0) - InputFolder, args(1) - OutputFolder 
indexer example:
spark-submit --deploy-mode cluster --master yarn --class Indexer path/to/indexer/scala.jar /nigeria/BD_assignment_1/ /nigeria/BD_assignment_1/indexer_result

ranker.scala  -  read JSON result from indexer, read Query, implement simple ranker and BM ranker, write results to csv files
ranker class: args(0) - InputFolder, args(1) - OutputFolder, args(2) - InputQuery
ranker example:
spark-submit --deploy-mode cluster --master yarn --class Ranker path/to/ranker/scala.jar /nigeria/BD_assignment_1/indexer_result /nigeria/BD_assignment_1/ranker_result "GOOGLE"

output csv files - http://10.90.138.32:9870/explorer.html#/nigeria/BD_assignment_1/

Trello project. https://trello.com/b/iTcqTfUx/big-data
