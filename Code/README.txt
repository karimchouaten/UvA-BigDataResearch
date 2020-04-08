The provided codes including a short description:

- Complete Player Load Query using Spark SQL: main code used to evaluate the performance of the whole query, using different amounts of workers ranging from 1 up to and until 16 workers on the computing cluster of Databricks.
- Performance per Sub-Query: code used to evaluate the performance of each individual sub-query (UNION element of the complete query).
- Scaling Datasources using Duplication: this code was used to scale the original datasources. Note: the datasets enclosed in this GitHub are NOT the original datasources, but an anonymised version with only the top 1000 records to prevent huge files.
- Datasources to Delta Tables: code used to convert the original datasources in Databricks to delta tables.