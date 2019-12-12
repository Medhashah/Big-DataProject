# big-data
2019 Fall Big Data Project

Task 1: Generic Profiling
Open data often comes with little or no metadata. You will profile a large collection of open data sets and derive metadata that can be used for data discovery, querying, and identification of data quality problems.

For each column in the dataset collection, you will extract the following metadata

1. Number of non-empty cells

2. Number of empty cells (i.e., cell with no data)

3. Number of distinct values

4. Top-5 most frequent value(s)

5. Data types (a column may contain values belonging to multiple types)

Identify the data types for each distinct column value as one of the following:

● INTEGER (LONG)

● REAL

● DATE/TIME

● TEXT

For each column count the total number of values as well as the distinct values for each of the above data types.

For columns that contain at least one value of type INTEGER / REAL report:

● Maximum value

● Minimum value

● Mean

● Standard Deviation For columns that contain at least one value of type DATE report:

● Maximum value

● Minimum value For columns that contain at least one value of type TEXT report:

● Top-5 Shortest value(s) (the values with shortest length)

● Top-5 Longest values(s) (the values with longest length)

● Average value length

Task 2: Semantic Profiling
For this task you will extract more detailed information about the semantics of columns. We will work for a subset of the data sets used in Task 1.

For each column, identify and summarize semantic types present in the column. These can be generic types (e.g., city, state) or collection-specific types (NYU school names, NYC agency). For each semantic type T identified, enumerate all the values encountered for T in all columns present in the collection.

You will look for the following types and add one or more semantic type labels to the column metadata together with their frequency in the column:

● Person name (Last name, First name, Middle name, Full name)

● Business name

● Phone Number

● Address

● Street name

● City

● Neighborhood

● LAT/LON coordinates

● Zip code

● Borough

● School name (Abbreviations and full names)

● Color

● Car make

● City agency (Abbreviations and full names)

● Areas of study (e.g., Architecture, Animal Science, Communications)

● Subjects in school (e.g., MATH A, MATH B, US HISTORY)

● School Levels (K-2, ELEMENTARY, ELEMENTARY SCHOOL, MIDDLE)

● College/University names

● Websites (e.g., ASESCHOLARS.ORG)

● Building Classification (e.g., R0-CONDOMINIUM, R2-WALK-UP)

● Vehicle Type (e.g., AMBULANCE, VAN, TAXI, BUS)

● Type of location (e.g., ABANDONED BUILDING, AIRPORT TERMINAL, BANK, CHURCH, CLOTHING/BOUTIQUE)

● Parks/Playgrounds (e.g., CLOVE LAKES PARK, GREENE PLAYGROUND)
