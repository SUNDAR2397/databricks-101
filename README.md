https://www.markdownguide.org/cheat-sheet/#basic-synta


from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize SparkSession (In Databricks, this is usually already done for you)
spark = SparkSession.builder.appName("FileProcessing").getOrCreate()

def process_file(file_path):
    # Load the file into a DataFrame
    df = spark.read.csv(file_path, header=True)  # Adjust this based on your file format and options
    
# Extract filename from the path (if full path is provided)
filename = file_path.split("/")[-1]
    
# Add a column with the filename
df = df.withColumn("filename", lit(filename))
    
# Perform additional data processing here as needed
    
return df  # You might want to return, save, or directly analyze the DataFrame
# Example of processing a specific file
file_to_process = "/path/to/your/file.csv"  # Adjust the path to your file
processed_df = process_file(file_to_process)

# You can now perform further analysis, save or display the processed DataFrame
