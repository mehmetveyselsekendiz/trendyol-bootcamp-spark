# trendyol-bootcamp-spark

Homework

Find the latest version of each product in every run, and save it as snapshot.

* Product data stored under the data/homework folder.
* Read data/homework/initial_data.json for the first run.
* Read data/homework/cdc_data.json for the nex runs.
* Save results as json, parquet or etc.
* Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.

Solution

The merged solution is saved to data/homework/snapshot_data folder.

+-------+------------+----------+---+----------------+------+-------------+
|  brand|    category|     color| id|            name| price|    timestamp|
+-------+------------+----------+---+----------------+------+-------------+
|    lcw|       shoes|      blue|  1|        product1| 100.0|1611141595000|
| adidas|       shoes|bone white|  2|product2_newName| 250.0|1611242395000|
|    lcw|Mobile Phone|     black|  3|        product3|  10.0|1611141595000|
|defacto|         hat|      blue|  4|product4_newName|  35.0|1611227995000|
|defacto|         hat|     green|  5|        product5|  33.0|1611141595000|
|    lcw|         hat|      blue|  6|        product6|  55.0|1611141595000|
|   dell|      Laptop|     white|  7|        product7|4500.0|1611141595000|
|  apple|      Laptop|     white|  8|      product8_1|5700.0|1611227995000|
|  apple|      Laptop|     black|  9|        product9|5000.0|1611141595000|
|  apple|Mobile Phone|     white| 10|       product10|1000.0|1611141595000|
|   nike|       shoes|bone white| 12|       product12| 200.0|1611262396000|
+-------+------------+----------+---+----------------+------+-------------+
