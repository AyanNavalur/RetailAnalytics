# Retail Analytics using Spark and AWS Services

Analyzing retail data from Cloudera Quick Start VM using Spark and AWS Services. </br>
Production ready code deployed on AWS EMR which fetches order data from AWS S3 and analyzes data containing more than 67,000 entries. </br></br>

To run this code create AWS EMR clusters having atleast 2 worker nodes. </br>
Also create source and target S3 buckets to host the retail data and output. </br>
Fetch retail data from github link - https://github.com/dgadiraju/retail_db and push it to source S3 bucket. </br>
Copy source code to AWS EMR master node using scp command (Linux) or WinSCP (Windows). </br></br>

Submit Spark job using command - spark-submit --master yarn --deploy-mode client RetailEnricher.py prod source sink </br>
Output is generated in the target S3 bucket.
