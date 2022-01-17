# deloitte-aws-databricks-jan-2022

## Spectrum 

```sql

create external schema gk_movies from data catalog 
database 'gk_db' 
iam_role 'arn:aws:iam::AAAAAAAAAAAAAAAAAAAAA:role/TRAINING_REDSHIFT_ROLE'
create external database if not exists;

SELECT * from gk_movies.movies; 

SELECT * from gk_movies.ratings_parquet;
```
