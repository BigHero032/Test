# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b1f41695-9eca-4125-b76b-f4bb66c66e65",
# META       "default_lakehouse_name": "AP",
# META       "default_lakehouse_workspace_id": "09999e1a-432f-4ce5-8f3c-c4242cc61db0",
# META       "known_lakehouses": [
# META         {
# META           "id": "b1f41695-9eca-4125-b76b-f4bb66c66e65"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Install a lightweight PDF text extractor (if not already installed)
%pip install pypdf


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Extract text from Lakehouse file and parse into PySpark DataFrame
import re
from pypdf import PdfReader
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. Path in Microsoft Fabric Lakehouse
# Files stored in 'Files/...' are mounted locally at '/lakehouse/default/Files/...'
pdf_path = "/lakehouse/default/Files/APVIDYUTH_ASSISTANT_EXECUTIVE_ENGINEER_ELECTRICAL_Score.pdf"

# 2. Extract lines from all PDF pages
reader = PdfReader(pdf_path)
extracted_lines = []

for page in reader.pages:
    text = page.extract_text()
    if text:
        for line in text.split("\n"):
            line = line.strip()
            if line:
                extracted_lines.append((line,))

# 3. Create initial Spark DataFrame from extracted lines
schema = StructType([StructField("raw_line", StringType(), True)])
raw_df = spark.createDataFrame(extracted_lines, schema)

# 4. Regex pattern matching:
# Group 1: Serial number (digits)
# Group 2: Hall Ticket No (e.g., APVR...)
# Group 3: Candidate Name
# Group 4: Score (float/decimal)
regex_pattern = r"^\s*(\d+)\s+([A-Z0-9]+)\s+(.+?)\s+(\d+\.\d+|\d+)\s*$"

# 5. Extract structured columns and cast types
scores_df = (
    raw_df.filter(F.col("raw_line").rlike(regex_pattern)).select(
        F.regexp_extract("raw_line", regex_pattern, 1)
        .cast(IntegerType())
        .alias("s_no"),
        F.regexp_extract("raw_line", regex_pattern, 2).alias("hall_ticket_no"),
        F.regexp_extract("raw_line", regex_pattern, 3).alias("candidate_name"),
        F.regexp_extract("raw_line", regex_pattern, 4)
        .cast(DoubleType())
        .alias("score"),
    )
)

# 6. Display DataFrame
scores_df.show(10, truncate=False)
# Write the extracted scores directly into Lakehouse Tables
scores_df.write.format("delta").mode("overwrite").saveAsTable("apvidyuth_scores")

# Count and view candidates scoring > 60
#above_60_df = scores_df.filter(F.col("score") > 60.0)
#print(f"Total candidates scoring > 60: {above_60_df.count()}")
#above_60_df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM AP.dbo.apvidyuth_scores")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
