# tipitaka

From 2021-2022 I studied Buddhist Studies at the University of Oxford.

My plan for my final research output had been to perform clustering on the entirety of the Pali canon.
Life took a different course. I realised that I was much more interested in data and software engineering than intensive academic study of Classical Chinese and Pali.

But now, I am returning to this project.
I am still interested in performing NLP on the enormous Pali canon (3x the length of the King James Bible).

For this project, I am building on the work of:
* Dan Zigmond, who performed the first computational analysis of the Pali canon in R.

You can find the paper:
http://jocbs.org/index.php/jocbs/article/view/236
(Use at your own risk, this website is not http secure.)

* The Pali text society, who have maintained the academic standard editions for the Pali canon.

My aims are to:
* Replicate Dan Zigmond's results with Python.
* Perform more extensive clustering, with further models and parameters. So far, KMeans and some limited similarity clustering techniques have been performed.
* Perform the first computational comparison between the Vipassana Institute and the Pali Text Society's editions of the Pali canon.


Project status:
* Data engineering - I have made a multi-zone data-model, which processes the data to be ingested for machine learning.
* For clustering, I will use an open-source tool `Metaflow` alongside AWS services (S3 & Batch).
Unfortunately, this will make cloning and replicating the results from the directory more difficult. I will not share the terraform files on this repo as I use the resources across different projects (but, all I have done is minorly changed wtanner's personal terraform files - big thank you to him). The advantages of horizontal/vertical scaling and repeatable machine learning DAGs are enormous, which Metaflow enables.
* I will finish with a writeup of my results. Hopefully, this will one day be published...


Project status:

Currently I am refactoring the ETL (often called 'preprocessing' by data scientists) to work with MetaFLow.


I have performed some preliminary clustering on the PTS Pali canon, which looks promising.

After the refactor, I am looking to:
* Build an ETL pipeline which loads the Vipassana edition data.
* Replicate Dan Zigmond's Agglomerative Clustering and visualisation in python (KMeans replication is completed).
* Perform a computational comparison of the Pali Text Society and Vipassana Edition Pali Canons.

Although this is a personal and academic project, the repository reflects some of my personal preferences for data and machine learning engineering:
* KISS - use object-oriented programming, distributed computing, nuanced database schemas and cloud computing _where appropriate_.
* With modern open source libraries, united by pyarrow, high-performance python is possible on a single machine.
* Open source tools and inexpensive cloud services for horizontal and vertical scaling.
* Pre-commit and Makefiles for ease of setup and code quality control (some of this is a little messy, and when working on other projects I would usually use CI/CD).
* Minimal notebooks. Jupyter Notebooks are great, particularly for learning and instruction. For productive projects with repateable code, I tend to keep away from them.

The data for this project all originates in open-source.


ingest -> tokenize-words -> clean -> preprocess -> kmeans
pre-raw -> raw -> curated -> staging -> clean
