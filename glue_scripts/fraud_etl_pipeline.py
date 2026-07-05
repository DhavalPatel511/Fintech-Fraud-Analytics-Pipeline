import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.functions import col,when,year, month, hour, sha2, regexp_replace, to_date, to_timestamp


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Cards Raw
CardsRaw_node1782007927165 = glueContext.create_dynamic_frame.from_catalog(database="fraud_db", table_name="cards", transformation_ctx="CardsRaw_node1782007927165")

# Script generated for node Users Raw
UsersRaw_node1782007928845 = glueContext.create_dynamic_frame.from_catalog(database="fraud_db", table_name="users", transformation_ctx="UsersRaw_node1782007928845")

# Script generated for node Transactions Raw
TransactionsRaw_node1782007929757 = glueContext.create_dynamic_frame.from_catalog(database="fraud_db", table_name="transactions", transformation_ctx="TransactionsRaw_node1782007929757")

# Script generated for node Mcc Raw
MccRaw_node1782007930480 = glueContext.create_dynamic_frame.from_catalog(database="fraud_db", table_name="mcc", transformation_ctx="MccRaw_node1782007930480")

# Script generated for node Cards Drop Duplicates
CardsDropDuplicates_node1782008485564 =  DynamicFrame.fromDF(CardsRaw_node1782007927165.toDF().dropDuplicates(), glueContext, "CardsDropDuplicates_node1782008485564")

# Script generated for node Users Drop Duplicates
UsersDropDuplicates_node1782008464197 =  DynamicFrame.fromDF(UsersRaw_node1782007928845.toDF().dropDuplicates(), glueContext, "UsersDropDuplicates_node1782008464197")

# Script generated for node Transactions Drop Duplicates
TransactionsDropDuplicates_node1782008410857 =  DynamicFrame.fromDF(TransactionsRaw_node1782007929757.toDF().dropDuplicates(), glueContext, "TransactionsDropDuplicates_node1782008410857")

# Script generated for node Mcc Drop Duplicates
MccDropDuplicates_node1782008438859 =  DynamicFrame.fromDF(MccRaw_node1782007930480.toDF().dropDuplicates(["id"]), glueContext, "MccDropDuplicates_node1782008438859")

# Script generated for node Card Silver
SqlQuery2066 = '''
SELECT
    CAST(id AS string) card_id,
    CAST(client_id AS string),
    card_brand,
    card_type,
    sha2(CAST(card_number AS STRING), 256) AS hashed_card_value,
    to_date(expires,'MM/yyyy') expires,
    has_chip,
    num_cards_issued,
    CAST(regexp_replace(credit_limit, '[$,]', '') AS float) AS credit_limit,
    to_date(acct_open_date, 'MM/yyyy') acct_open_date,
    to_date(year_pin_last_changed,'yyyy') year_pin_last_changed
FROM myDataSource

'''
CardSilver_node1782010831591 = sparkSqlQuery(glueContext, query = SqlQuery2066, mapping = {"myDataSource":CardsDropDuplicates_node1782008485564}, transformation_ctx = "CardSilver_node1782010831591")

# Script generated for node Users Silver
SqlQuery2067 = '''
SELECT
    CAST(id AS string) client_id,
    current_age,
    retirement_age,
    gender,
    CAST(regexp_replace(per_capita_income, '[$,]', '') AS float) AS per_capita_income,
    CAST(regexp_replace(yearly_income, '[$,]', '') AS float) AS yearly_income,
    CAST(regexp_replace(total_debt, '[$,]', '') AS float) AS total_debt,
    credit_score,
    num_credit_cards,
    CASE WHEN credit_score <= 579 THEN "Poor"
     WHEN credit_score >= 580 AND credit_score <= 669 THEN "Fair"
     WHEN credit_score >= 670 AND credit_score <= 739 THEN "Good"
     WHEN credit_score >= 740 AND credit_score <= 799 THEN "Very Good"
     ELSE  "Excellent" END AS credit_range,
    CASE WHEN current_age < 18 THEN "Under 18"
     WHEN current_age >= 18 AND current_age <= 29 THEN "Young Adults"
     WHEN current_age >= 30 AND current_age <= 55 THEN "Established Borrowers"
     ELSE  "Older Borrowers" END AS age_range
FROM myDataSource

'''
UsersSilver_node1782013205723 = sparkSqlQuery(glueContext, query = SqlQuery2067, mapping = {"myDataSource":UsersDropDuplicates_node1782008464197}, transformation_ctx = "UsersSilver_node1782013205723")

# Script generated for node Transaction Silver1
SqlQuery2068 = '''
SELECT
	CAST(id AS string) transaction_id,
	to_timestamp(date,"yyyy-MM-dd HH:mm:ss") transaction_date,
	CAST(client_id AS string) client_id,
	CAST(card_id AS string) card_id,
	CAST(regexp_replace(amount, '[$,]', '') AS float) AS amount,
	use_chip,
	CAST(merchant_id AS string) merchant_id,
	merchant_city,
	merchant_state,
	CAST(zip AS string) zip,
	CAST(mcc AS string) mcc_id,
	errors
FROM myDataSource

'''
TransactionSilver1_node1782013876776 = sparkSqlQuery(glueContext, query = SqlQuery2068, mapping = {"myDataSource":TransactionsDropDuplicates_node1782008410857}, transformation_ctx = "TransactionSilver1_node1782013876776")



# Script generated for node Mcc Silver
MccSilver_node1782015694397 = ApplyMapping.apply(frame=MccDropDuplicates_node1782008438859, mappings=[("id", "long", "id", "string"), ("category", "string", "category", "string")], transformation_ctx="MccSilver_node1782015694397")

fraud_df = glueContext.create_dynamic_frame.from_catalog(
    database="fraud_db", table_name="fraud"
).toDF()
# Joining all the tables

transaction_df = TransactionSilver1_node1782013876776.toDF()

users_df = UsersSilver_node1782013205723.toDF()

cards_df = CardSilver_node1782010831591.toDF()

mcc_df = MccSilver_node1782015694397.toDF()

transaction_df = transaction_df.join(fraud_df, "transaction_id", "left")

joined_df = (
    transaction_df
    .join(users_df, transaction_df.client_id == users_df.client_id, "left")
    .join(cards_df, transaction_df.card_id == cards_df.card_id, "left")
    .join(mcc_df, transaction_df.mcc_id == mcc_df.id, "left")
)

final_df = joined_df.select(
	transaction_df["transaction_id"],
	transaction_df["transaction_date"],
	transaction_df["client_id"],
	transaction_df["card_id"],
	"amount",
	"use_chip",
	"merchant_city",
 	"merchant_state",
 	"errors",
 	"gender",
 	"per_capita_income",
 	"yearly_income",
 	"total_debt",
 	"num_credit_cards",
 	"credit_range",
 	"age_range",
 	"card_brand",
 	"card_type",
 	"has_chip",
 	"num_cards_issued",
 	"credit_limit",
 	"category",
 	"is_fraud")

final_df = (final_df.withColumn("transaction_type",when(col("amount") < 0,"refund").otherwise("purchase")))
final_df = (final_df.withColumn("year",year("transaction_date")).withColumn("month",month("transaction_date")).withColumn("hour",hour("transaction_date")))
final_df = final_df.withColumn(
    "fraud_label",
    when(col("is_fraud") == "Yes", "fraud")
    .when(col("is_fraud") == "No", "legitimate")
    .otherwise("unknown")
)

final_df.write.mode("overwrite").partitionBy("year","month").parquet("s3://batch-fraud-etl-pipeline-processed/transactions/")
job.commit()
