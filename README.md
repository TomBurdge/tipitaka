# tipitaka

From 2021-2022 I studied Buddhist Studies at the University of Oxford.

My plan for my final research output had been to perform clustering on the entirety of the Pali canon.
Life took a different course. I realised that I was much more interested in data and software engineering than intensive academic study of Classical Chinese and Pali.

But now, I am returning to this project.
We all stand on the shoulders of giants. For this project, most notable are:
* Dan Zigmond, who performed the first computational analysis of the Pali canon in R.

You can find the paper:
http://jocbs.org/index.php/jocbs/article/view/236
(Use at your own risk, this website is not http secure.)

* The Pali text society, who have maintained the academic standard texts for the Pali canon

My aims are to:
* Replicate Dan Zigmond's results with Python.
* Perform more extensive clustering, with further models and parameters. So far, KMeans and some limited similarity clustering techniques have been performed.
* Perform the first computational comparison between the Vipassana Institute and the Pali Text Society's editions of the Pali canon.


Project status:
* Data engineering - I have made a multi-zone data-model, which processes the data to be ingested for machine learning.
* For clustering, I will use an open-source tool `Metaflow` alongside AWS services (S3 & Batch).
Unfortunately, this will make cloning and replicating the results from the directory more difficult. MetaFlow uses Anaconda and this repo generally uses Poetry, and I will not share the terraform files on this repo as I use the resources across different projects (but, all I have done is minorly changed wtanner's personal metaflow files - big thank you to him). But the advantages of convenient horizontal and vertical scaling is too great.
* I will finish with a writeup of my results. Hopefully, this will one day be published...


There are still some English language words passing through to the clean zone. It is somewhat precarious to remove English words without also removing Pali words. I will correct this before I begin performing extensive clustering.
(Initial clustering results look promising. There are some serious issues with the number of features and overfitting, but the research still reflects the results being replicated. I am planning to take a look at the original R findings and checking where in the ML workflow PCA is implemented).


Although this is a personal and academic project, the repository reflects some of my personal preferences for data and machine learning engineering:
* KISS - use object-oriented programming, distributed computing, and cloud computing where appropriate.
* With modern open source libraries, united by pyarrow, high-performance python is possible on a single machine.
* Open source tools and inexpensive cloud services for horizontal and vertical scaling (this, of course, changes for non-personal projects).
* Pre-commit and Makefiles for ease of setup and quality control (some of this is a little messy, and when working on other projects I would usually use CI/CD)

The data for this project all originates in open-source.
