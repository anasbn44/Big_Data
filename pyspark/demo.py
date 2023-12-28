from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date

# Initialize SparkSession
spark = SparkSession.builder.appName("MySQLIntegration").getOrCreate()

# Define MySQL configurations
mysql_config = {
    "url": "jdbc:mysql://localhost:3306/db_imomaroc",  # Replace with your database URL
    "driver": "com.mysql.jdbc.Driver",
    "dbtable": "projets",  # Replace with the name of the table you want to load
    # Add other necessary configurations like username, password, etc. if required
    "user": "root",
    "password": "",
}

# Load MySQL table into a PySpark DataFrame
projets_df = spark.read.format("jdbc").options(**mysql_config).load()

# Show the loaded data
# projets_df.show()



# Assuming date_fin is the end date column in projets_df
# projets_en_cours = projets_df.filter(current_date() >= projets_df.date_debut) \
#     .filter(current_date() <= projets_df.date_fin)
#
# projets_en_cours.show()

if __name__ == "__main__":
    projets_df.show()

