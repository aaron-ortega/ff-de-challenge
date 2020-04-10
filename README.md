# Farmer's Fridge Data Engineering Coding Exercise

## Problem Statement

As a data engineer within the Farmer's Fridge tech team, you will be responsible for building a robust and scalable data platform while also working within a cross-functional capacity with various teams on their unique data needs. Often times, this will entail bringing in new data, integrating disparate datasets, and cleaning and shaping the data.

As a company, Farmer's Fridge operates a network of 400+ smart fridges in easily accessible locations in various verticals such as hospitals, airports, office buildings, and schools. In this exercise, the analytics team is interested in understanding the density and walkability of the fridges given that the vast majority of our customer interactions occur when someone walks by a fridge and decides to purchase an item. The csv file included is a sample of our fridges and their corresponding geo coordinates. Please use this dataset as the starting point for the exercise described below.

## Coding Exercise

1. Profile the sample dataset and determine what preprocessing are needed to ensure the cleanliness and integrity of the dataset. Describe your findings and what you did to clean the data.

2. Create an automated process that can take in the fridge location data and integrate with an external data source or API (e.g. mapbox free tier) that can provide the geo data to determine the area reachable on foot within 5 minutes, and 10 minutes from any fridge.

3. Plot the results in your preferred way to generate a visualization.

4. Identify where the largest overlap areas are (if any).

5. Describe how you would productionalize this pipeline in AWS and what things you would consider.

Bonus: Create the solution in a way that can be scaled up to millions of entries.

Bonus 2: Describe how you would enhance this exercise to make the analysis more useful.

## Submission

Please send your completed solution in a zipped git repo including any supporting documentation, tests, and setup that you think is necessary. It is important for us to see the commit history and get a sense of how you write code.

## Evaluation

You do not need to complete the exercise and we are not looking for a specific answer or solution, but are looking to understand how you approach a data engineering problem and the considerations you took. Spend enough time to at least have a framework in place and be able to speak to the intricacies of the solution. Please write your own code instead of reusing existing code library or project. We want to see what you consider to be clean, workable and easily-maintainable code.
