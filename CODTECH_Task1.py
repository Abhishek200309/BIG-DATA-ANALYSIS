# task1_pyspark_movies_scaling_exploded_html.py
import time
import webbrowser
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -------------------------
# 1) Start Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("Task1_PySpark_Movies_Scaling_Exploded_HTML") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

# -------------------------
# 2) Load Dataset
# -------------------------
file_path = "D:/197001__/movies_dataset.csv/movies_dataset.csv"   # update path
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("\n✅ Schema of dataset:")
df.printSchema()

# -------------------------
# 3) Clean + Explode Genres
# -------------------------
df = df.withColumn("GenreArray", F.split(F.col("Genre"), "\\|"))
df = df.withColumn("GenreExploded", F.explode("GenreArray"))
df = df.select("User_Id", "Movie_Name", "Rating", F.col("GenreExploded").alias("Genre"))

# -------------------------
# 4) Run Analysis at Different Sizes
# -------------------------
results = {}
total_rows = df.count()
sizes = [100_000, 500_000, total_rows]

for size in sizes:
    if size < total_rows:
        df_sample = df.limit(size).cache()
    else:
        df_sample = df.cache()
    df_sample.count()  # materialize cache

    start = time.time()

    avg_rating_by_genre = df_sample.groupBy("Genre") \
        .agg(F.avg("Rating").alias("avg_rating")) \
        .orderBy(F.desc("avg_rating")) \
        .toPandas()

    top_movies = df_sample.groupBy("Movie_Name") \
        .agg(F.avg("Rating").alias("avg_rating")) \
        .orderBy(F.desc("avg_rating")) \
        .limit(5) \
        .toPandas()

    rating_distribution = df_sample.groupBy("Rating") \
        .count().orderBy("Rating") \
        .toPandas()

    popular_genres = df_sample.groupBy("Genre") \
        .count().orderBy(F.desc("count")) \
        .limit(10) \
        .toPandas()

    stats = df_sample.agg(
        F.mean("Rating").alias("mean_rating"),
        F.stddev("Rating").alias("stddev_rating")
    ).collect()[0]

    duration = time.time() - start

    results[size] = {
        "avg_rating_by_genre": avg_rating_by_genre,
        "top_movies": top_movies,
        "rating_distribution": rating_distribution,
        "popular_genres": popular_genres,
        "stats": stats.asDict(),
        "time": duration
    }

# -------------------------
# 5) Generate HTML Report
# -------------------------
html_content = """
<html>
<head>
    <title>🎬 Movie Dataset Analysis Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
        h1 { color: #2c3e50; }
        h2 { color: #34495e; margin-top: 30px; }
        table { border-collapse: collapse; width: 80%; margin-bottom: 20px; }
        table, th, td { border: 1px solid #ccc; padding: 8px; }
        th { background: #2c3e50; color: white; }
        tr:nth-child(even) { background: #f2f2f2; }
    </style>
</head>
<body>
<h1>🎬 Movie Dataset Analysis Report</h1>
"""

for size, res in results.items():
    html_content += f"<h2>Dataset Size: {size:,} rows</h2>"
    html_content += f"<p><b>Time taken:</b> {res['time']:.2f} seconds</p>"
    html_content += f"<p><b>Mean Rating:</b> {res['stats']['mean_rating']:.2f}, "
    html_content += f"<b>StdDev:</b> {res['stats']['stddev_rating']:.2f}</p>"

    html_content += "<h3>⭐ Average Rating by Genre</h3>"
    html_content += res["avg_rating_by_genre"].to_html(index=False)

    html_content += "<h3>🎥 Top 5 Movies</h3>"
    html_content += res["top_movies"].to_html(index=False)

    html_content += "<h3>📊 Rating Distribution</h3>"
    html_content += res["rating_distribution"].to_html(index=False)

    html_content += "<h3>🔥 Most Popular Genres</h3>"
    html_content += res["popular_genres"].to_html(index=False)

html_content += "</body></html>"

report_path = "D:/197001__/movies_scaling_report.html"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(html_content)

webbrowser.open(f"file://{report_path}")

# -------------------------
# 6) Stop Spark Session
# -------------------------
spark.stop()
