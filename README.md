# BIG-DATA-ANALYSIS

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: ABHISHEK VICTOR RAJ MANESH

*INTERN ID*: CT12DY2725

*DOMAIN*: DATA ANALYTICS

*DURATION*: 12 WEEKS

*MENTOR*: NEELA SANTOSH

# Description

This project demonstrates the use of PySpark for analyzing a large-scale movie ratings dataset. With the rapid growth of data in modern applications, traditional tools like Excel or pandas often struggle to handle millions of rows efficiently. PySpark, built on top of Apache Spark, allows distributed data processing, enabling fast and scalable analysis of big datasets.

The dataset contains user ratings for thousands of movies, along with their genres. Many movies belong to multiple genres, separated by a pipe (|) character. This multi-genre structure requires careful preprocessing to ensure accurate analytics. In this project, we explode the genre column so that each movie is represented once per genre. This ensures that metrics such as average ratings per genre are precise and representative of user preferences.

Key features of this project:

The project performs multi-scale analysis on different dataset sizes, including subsets of 100,000 rows, 500,000 rows, and the full dataset. This approach allows both insight extraction and performance benchmarking, demonstrating how PySpark can efficiently process varying amounts of data.

The analysis includes:

- Average Rating by Genre: Identifies which genres receive higher or lower ratings from users, helping to understand genre popularity.

- Top 5 Movies: Extracts the highest-rated movies to highlight critical or audience favorites.

- Rating Distribution: Shows how users rate movies overall, identifying trends such as skewed or uniform rating patterns.

- Most Popular Genres: Determines which genres have the most ratings, indicating audience engagement across categories.

Caching intermediate DataFrames in memory ensures that repeated computations are fast, demonstrating an important performance optimization technique in PySpark. Additionally, the time taken for each dataset size is measured, allowing users to compare efficiency and understand scalability.

# HTML Report Generation

All results are exported to a comprehensive HTML report, making the analysis visually accessible. The report includes:

- Tables showing average rating per genre

- Top 5 movies with ratings

- Rating distribution for quick insights

- Popular genres with count information

- Dataset size and processing time

The HTML format ensures that the report can be opened in any web browser and shared easily with stakeholders, analysts, or team members without needing PySpark installed.

# Technical Highlights

This project showcases key big data and PySpark concepts, including:

- Data cleaning and transformation (splitting and exploding multi-genre movies)

- Grouping and aggregation with groupBy and agg

- Performance optimization using caching

- Conversion of Spark DataFrames to pandas for HTML export

- Multi-scale analysis to study dataset performance and insights

By combining PySpark with Python’s reporting capabilities, the project illustrates how data engineers and analysts can process large datasets, extract meaningful metrics, and present results in a reproducible, user-friendly format.

# Dataset

The dataset should be a CSV file with at least the following columns:

 - User_Id – Unique identifier for the user

 - Movie_Name – Name of the movie

 - Genre – Pipe-separated list of genres (e.g., Action|Comedy)

 - Rating – Numeric rating given by the user

# Output

The HTML report contains:

 - Dataset size and time taken for analysis

 - Mean and standard deviation of ratings

 - Average rating by genre

 - Top 5 movies by rating

 - Rating distribution

 - Most popular genres


<img width="1542" height="839" alt="Image" src="https://github.com/user-attachments/assets/20515bc7-62bf-443e-ab59-a1d4c5dc0aac" />
<img width="1528" height="608" alt="Image" src="https://github.com/user-attachments/assets/5a37f8fe-ef65-45bb-936f-59c0a34cb378" />
<img width="1526" height="381" alt="Image" src="https://github.com/user-attachments/assets/37548f2f-fe7a-49e4-9e2a-2d6237549506" />
<img width="1530" height="775" alt="Image" src="https://github.com/user-attachments/assets/48fe1ddf-15fb-4a1d-b8a3-a4afe647ad3d" />
<img width="1526" height="613" alt="Image" src="https://github.com/user-attachments/assets/7d9a0797-706c-47a4-9cf2-39b1a8620e19" />
<img width="1529" height="377" alt="Image" src="https://github.com/user-attachments/assets/892f0cf7-fd67-4012-8ee7-dbcd237a82f4" />

# Conclusion

This project serves as a practical example of scalable data analysis with PySpark, handling multi-genre datasets efficiently while producing actionable insights. It highlights the importance of distributed computing for modern data workflows and provides a solid foundation for building large-scale analytics pipelines, recommendation engines, or reporting dashboards.
<img width="1524" height="777" alt="Image" src="https://github.com/user-attachments/assets/b68eff20-9319-4e4c-b7cd-9048eecec653" />
<img width="1525" height="610" alt="Image" src="https://github.com/user-attachments/assets/c2a56cfb-0452-41ca-bd5f-45c045b77a83" />
<img width="1523" height="375" alt="Image" src="https://github.com/user-attachments/assets/c684f70e-8df7-49ec-b927-81e6b77d2957" />
