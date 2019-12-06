CS435_TERM PROJECT
=================
Authors:
    Edward Lee, Laksheen Mendis and Suraj Eswaran, Colorado State University
    
Required Software
================
Running the code in this repository requires the installation of:
    1. Apache Spark
    2. Intellij or any other IDE
    3. IDLE or any ohter IDE
    
 Problem Statement
=================
In large cities in the modern day, crime is a rampant problem. In cities like New York, Denver, Chicago, and San Francisco, residents and visitors alike must be wary of the possible crimes and incidents that can occur. From robbery to assault, dangers in these cities can be diverse and unique. 
The United States has a lot of data, from census data to crime statistics that could be used for accurately predicting crimes across the country. From big data technology, it can be possible to  search records, looking for similar crimes with similar scenarios, and tying them together. Fields like the crime’s location, date, and time all can play a role in predicting and relating different crimes. And outside influences such as the area’s population, economic status, and weather conditions can play an indirect role in an area’s crime rates. Because of the amount of information to be processed, Big Data can be used as a new weapon against crime .
Our goal for this project is to provide a way to more accurately predict incidents across the San Francisco area. For residents, visitors, and police departments, it can be beneficial to have a way to predict what kind of crimes are going to take place in a certain area. Residents and visitors could plan their routes ahead of time to avoid certain areas. Police departments could use this information to better manage their resources to resolve incidents and increase the safety of each area. For example, police departments could patrol certain areas more at certain times of the day, in order to reduce the number of incidents and to respond to such incidents quicker.

APPROACH
=================
We broke down our project into several steps: 
1. Process the census data down to only include the San Francisco area in California. This also includes organizing the census data with the zip code as the key. 
2. Process the incident data, to link each incident with a particular zip code in San Francisco. 
3. Perform the final merging of the datasets, extracting key information from each dataset, such as population, average income levels, incident category and incident severity.
4. Based on the processed incident dataset, train a machine learning model to predict type of incident that would occur.

DATASETS
=================
1. OpenData. (2019). Police Department Incident Reports: 2018 to Present. [Data file of Incident Reports]. Retrieved from https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783
2. United States Census Bureau. (2002). Summary File 3 Dataset. [Data files containing census information for the state of California]. Retrieved from https://www2.census.gov/census_2000/datasets/Summary_File_3/California/

