# Big Data Analytics Using Spark

Programming Assignments and Examples from UC San Diego - DSE230X
>https://www.edx.org/course/big-data-analytics-using-spark

## Key Project/Notebook

### Weather Data Analysis Using Big Data and PySpark

This repository features a comprehensive suite of tools and notebooks dedicated to the analysis of large-scale weather datasets using Big Data technologies. A highlight of this collection is the "Weather Analysis Coursework" notebook, which exemplifies the power of PySpark in processing and visualizing extensive weather data. Key features include:

**Data Integration and Preparation:** Efficient retrieval and merging of weather data with geographical information.

**Data Smoothing and Trend Analysis:** Application of Gaussian smoothing techniques to reveal long-term weather patterns.

**Statistical Analysis:** Detailed computation of mean, standard deviation, and PCA to understand data variability and principal components.

**Visualization:** Advanced plotting to visualize smoothed data, statistical measures, and eigenvectors.

**Big Data Techniques:** Leveraging PySpark's distributed computing capabilities for efficient data processing and analysis.

This project not only showcases the use of PySpark for handling large datasets but also provides valuable insights into weather patterns and trends, making it a vital component of this repository. These are skills I developed taking this course. 

## General Notes About Spark and Data Analysis

* ## Resilient Distributed Datasets (RDD)
   * Think of RDD as data in a list distributed among executors or different computers.
   * It is a distributed immutable array.
   * The Spark Context / Head Node is our gateway to these RDDs from our main program.
   ---
   * Map - Applies operation to each element of RDD in parallel.
   * Reduce - Uses reduce operators on all executors to give one final output.
   * Reduce operator takes two inputs, gives one output.
   * Filter - Selectively chooses some elements out of total.
   ---
   * Parallelize - Turns a list to an RDD.
   * Collect - Inverse operation that materializes RDD.
   ---
   * Lazy Evaluation - Instead of mapping all data, then reducing all of it (which goes through data twice)
   * Map first value, reduce it , map second value, reduce it and update the result, ...
   * When same RDD is needed for different computations use .cache() after .map()
   * This is in contrast to lazy eval. Spark usually doesn't cache but uses pipelined eval (lazy).
   ---
   * Partitioning used to dedicate workers to parts of RDD.
   * glom() can be used to investigate each partition.
   ---
   
* ## Spark SQL
   * Dataframes are more restricted than RDDs.
   * It is essentially an RDD of rows , plus additional information of schema.
   ---
   * Dataframe can be created from an RDD that is a list of rows.
   * If schema is explicitly passed it can be created from tuples. (Usual content of RDD)
   ---
   * Parquet is an underlying data structure on disk from which we can load dataframes.
   * Spark SQL can query a database without going through all of it.
   ---
   * Imperative manner of manipulation in SQL specifies both what we want, and how to get it. We specify what order in which to perform      operations. 
   * Declarative only needs what, so that compiler can optimize how. (SELECT FROM WHERE syntax)
   * Aggregations can be used to get some statistics on dataframe. (Like approximate count)
   ---
   
 * ## Practical Aspects
   * Datasets may contain missing information. It is important to see where the data is dense.
   * Dataframes allow user defined functions, similar to maps, that we can use to detect NaNs.
   ---
   * Data is serialized for storage, whether persistent or on HDFS.
   * When deserializing, parse and check for errors in parsing.
   ---
   
 * ## Principle Component Analysis
   * Functions can be approximated with orthonormal basis functions.
   * Orthonormal functions can be used to recover the original curve from noise added to it.
   ---
   * PCA consists of 2 steps -
     * Computing the covariance matrix.
     * Computing the eigenvalue decomposition.
   ---
   * NaNs in matrices to be substituted or ignored for calculations.
   * To compute mean, and expectation together, put a 1 in front of the initial vector, then matrix multiply.
   ---
   * Exploratory Analysis -
     * Calculate mean and standard deviation. Plot mean +- SD.
     * Plot percentage of variance explained by top eigenvectors. This gives a sense of how effective they are in approximating the            data.
     * Plot the top eigenvectors themselves to get insights.
   ---
   * Residuals can be calculated by subtracting the eigenvectors from previous residuals. The 0th residual is the vector minus its mean.
   * Each residual is normalized. The smaller the residual norm, the better the approximation.
   * More residuals means using more eigenvectors. The best fit and worst fit can be checked. Worst fit may show outliers due to lack of      data.
   * It is possible that the noise in the data will not be reconstructed.
   ---
   *  Scatter plots cannot represent the density of data effectively. Use box plots.
   *  It may be useful to have a scatter plot between different coefficients of eigenvectors.
   --- 
   * To analyze the effect of a component on a residual, we can check the RMS after subtracting the mean over that component.
   * We can iteratively reduce the RMS by alternatingly subtracting mean of different components. 

 * ## K Means
   *  Candidates for centers initialized, and broadcast.
   *  In each executor, data is partitioned according to closest candidate. Key Value pair - (candidate, data value)
   *  Mean with reduce by key, and new candidates broadcast again.
   *  K Means fails when the intrinsic dimensions of data is high - curse of dimensionality.





   
   
   
