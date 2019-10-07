# BD_SearchEngine

Simple Search Engine in Spark. https://hackmd.io/MK6b5hQgQm6j-4KwVWZuQA?view

indexer.scala  -  read JSON files, count words, calculate TF, IDF, unify tables, write to JSON files
indexer class: args(0) - InputFolder, args(1) - OutputFolder 

ranker.scala  -  read JSON result from indexer, read Query, implement simple ranker and BM ranker, write results to csv files
ranker class: args(0) - InputFolder, args(1) - OutputFolder, args(2) - InputQuery

output csv files - http://10.90.138.32:9870/explorer.html#/nigeria/BD_assignment_1/

Trello project. https://trello.com/b/iTcqTfUx/big-data
