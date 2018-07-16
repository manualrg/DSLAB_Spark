
#   SPARK TEXT ANALYTICS LAB: ENRON EMAILS

## Introduction

Enron Corporation was the leading energy company in late 90's in the USA. They knew how to integrate their energy business (power generation, distribution, natural gas trading, etc) and leverage the energy deregularization process during the 90's decade. Moreover, they were able to develop innovative and radical business ideas. Nevertheless, they take it too far and Enron went banckrupty in 2001. They were accused of accounting fraud, spreading an energy crisis and energy market manipulation.

The consequences were of the utmost importance, it was the greatest corporate scandal until Leman Brothers in 2008. Besides being the main cause of the famous rolling blackouts in California and spreading world wide fear to rederegulated energy markets, it caused the dissolution of one of the biggest accounting firm Arthur Andersen.

In this repository,  we will analyze a subset of Enron's email database that was released after the trial. The dataset is classified as useful at trial or not, We will discover more deeply the whole story by exploring and analyzing the corpus, and then, we will go use several machine learning techniques applied to text analytics to that help us in our task: To perform a binary classification of a really big email database by it's relevance at trial.

## The data
The dataset is from EDx course: The Analytics Edge:

https://www.edx.org/es/course/analytics-edge-mitx-15-071x-3


## Inverted Index and Weigth of Evidence EDA

Our main goal in the first notebooks is to explore and get to know the data, in order to get useful insight to leverage in subsequent steps. Firstly,  an Inverted - Index will be computed, it is a data structure that maps every word (term or token) to the list of documents were it apperas. It is a very useful tool that allows use to handle unstructured data and begin with our exploration.

Seconldy, we will compute Weigth of Evidence. This metric will tell us every token predictive strength and the direction of the relationship with the response variable.

On the other hand, we will be using some visualization tools such us Brunel (in Spark Scala), PixieDust (in PySpark) or matplotlib (when our data is collected)


## BOW Featurization (TF-IDF) and Document Binary Classification

The next step is to take advantage of our understanding and discoveries achived during our exploration and to develop a simple classification model. This notebook covers data cleaning, featurization, classification model building and assessment. 

We will build a document-term matrix by performing a BOW Featurization. It is a frequentist approach that does not take into account semantic relationships, however, it is very effective and a good example of parsimony (the simplest yet effective enough model, able to generalize to unseen data). Then, we will build a logistic regression and will assess it's results, this model will provide a baseline to allow model benchmarking.

## Advanced Feature Engineering Techniques

 

## Platform and dependencies
Notebooks have been developed with IBM DSX platform in Spark (both Spark Scala and PySpark): 

https://datascience.ibm.com/

Of course they can run in every other platform. However, there are two depencencies:
* Brunel: https://github.com/Brunel-Visualization/Brunel
* PixieDust: https://github.com/ibm-watson-data-lab/pixiedust

Spark version 2.1
Python version 2.7
Scala version 2.11

```python

```
