# Data Validation
Validate csv file data based on column rule set given in json file and segregate data in two file:
success file - with all valid records.
error file - with all invalid record with their error description

# Resources
Input Output File And Rule Set Json file is maintained in Data Folder.

# Key Features
 1) This is analytics.conf driven job where you can manage your input output and rule set json file path accordingly.
 2) In this code we can update validation function as per requirement in future.
 3) This is complete imputable code.
 4) Logger is implemented in job to track as per requirement.
 5) Best Practices Followed  - TDD and Doc String for all functions. 
 
# Future Scope Of Improvement
1) Better Data Validation Function covering all edge case.
2) Architecture design of code can be improved.

# Alternative Approach
Just iterate over rdd and make all validation check.
But, here I used DataFrame for better performance.

# Spark Submit Command 
spark-submit --master yarn-cluster--executor-memory 15G --num-executors 7 --executor-cores 3
 --driver-java-options "-Duser.timezone=UTC" --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC"
  --class com.peak.analytics.users.executor.DataValidationSparkJob "jar path"
