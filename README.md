# Data Engineering Capstone Project
## Overview

The purpose of the data engineering capstone project is to give you a chance to combine what you've learned throughout the program. This project will be an important part of your portfolio that will help you achieve your data engineering-related career goals.

In this project, you can choose to complete the project provided for you, or define the scope and data for a project of your own design. Either way, you'll be expected to go through the same steps outlined below.

#### Udacity Provided Project

In the Udacity provided project, you'll work with four datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics, and temperature data. You're also welcome to enrich the project with additional data if you'd like to set your project apart.

#### Open-Ended Project

If you decide to design your own project, you can find useful information in the Project Resources section. Rather than go through steps below with the data Udacity provides, you'll gather your own data, and go through the same process.

## Instructions
To help guide your project, we've broken it down into a series of steps.

### Step 1: Scope the Project and Gather Data

Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

1. I have sourced my own datasets for this project:
  * UK MOT testing data results (2020) 'results'

    http://data.dft.gov.uk/anonymised-mot-test/test_data/dft_test_result_2020.zip

  * UK MOT testing data failure item (2020) 'items'

    http://data.dft.gov.uk/anonymised-mot-test/test_data/dft_test_item_2020.zip

    *Note that although both datasets are provided in zipped batches of .csv files, for  the purposes of this submission the 'data failure item' dataset has been converted into json.

#### Initial observations of the data:

* The items dataset has 75,907,074 rows, and the results dataset has 38,594,013
* The datasets come with a set of lookup tables
* The datasets come with a document explaining the structure, and any acronyms.

#### Use Cases for the data:

This data will loaded into an analytics table in Amazon Redshift to allow for analysis across the data in aggregate, such as:

* Plotting MOT failure rate against car age

* Analysing the cars which fail MOT's most often

* Identifying the most common cars in the UK which are subject to MOT

### Step 2: Explore and Assess the Data

3. Each dataset is explored in the Investigation.ipynb notebook. To summarise:
  * The datasets appear to be easily joined on the 'test_id' field, which appears to be the primary key for the results dataset.

  * Neither table has any NaN values, however there are large numbers of Null values:
    * The items dataset has 71,960,197 NULL values in the 'dangerous_mark' column. Documentation suggests this is how a 'non dangerous' result is recorded. Dangerous results are marked as 'D'

    * The results dataset has 1,007,292 NULL values in the 'test_mileage column', 77,014 in the 'cylinder_capacity' column and 631 in the 'first_use_date column'. At most, this represents less than 3% of this dataset.

4. The following steps should be taken to clean the data during ETL:
  * The null entries in the 'dangerous_mark' field should be changed to 'N'.

  * As the NULL values in the results dataset only represent less than 3% of the data, these rows should be dropped.


### Step 3: Define the Data Model

5. The data will be mapped to the following ERD:

![ERD](https://github.com/sama26/UDE_capstone_project/blob/main/MOT_Database.jpeg)

This is similar to the source schema but creates a new dimension table for the MOT vehicle. This results in a star schema, containing a fact table 'test_result', and 2 dimension tables; 'test_item' and 'vehicle'.

The test_item table will need to have an additional ID field to serve as a primary key

The lookup tables included with the dataset could be included as additional dimension tables at a later date if required, or simply used to enrich the existing tables.

6. The following steps will be taken to pipeline  the data into this data model:

  1. The dataset files will be loaded into an S3 bucket in the us-west-2 region

  2. The files will be loaded into staging tables within a Redshift cluster in the same region

  3. Pyspark queries will be run to create the new fact and dimension tables

  4. Data Quality checks will be carried out on all tables.

  5. This will be orchestrated by an Airflow DAG running on a local computer

### Step 4: Run ETL to Model the Data

7. Create the data pipelines and the data model
8. Include a data dictionary
9. Run data quality checks to ensure the pipeline ran as expected:
    * Integrity constraints on the relational database (e.g., unique key, data type, etc.)

    * Unit tests for the scripts to ensure they are doing the right thing

    * Source/count checks to ensure completeness

### Step 5: Complete Project Write Up

10. What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?

11. Clearly state the rationale for the choice of tools and technologies for the project.

12. Document the steps of the process.

13. Propose how often the data should be updated and why.

14. Post your write-up and final data model in a GitHub repo.

15. Include a description of how you would approach the problem differently under the following scenarios:

  * If the data was increased by 100x.

  * If the pipelines were run on a daily basis by 7am.

  * If the database needed to be accessed by 100+ people.
