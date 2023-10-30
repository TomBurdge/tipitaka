# tipitaka

From 2021-2022 I studied at the University of Oxford.

My plan for my final research output had been to perform clustering on the entirety of the Pali canon.
Life took a different course. I realised that I was much more interested in data and software engineering than intensive study of Classical Chinese and Pali.

But now, I am returning to this project.
I am standing on the shoulders of giants. These are:
* Dan Zigmond, who performed the first computational analysis of the Pali canon in R.

You can find the paper:
http://jocbs.org/index.php/jocbs/article/view/236
(Use at your own risk, this website is in fact not http secure.)

* The Pali text society, who have maintained the academic standard texts for the Pali canon

My aims are to:
* Replicate Dan Zigmond's results with Python.
* Perform more extensive clustering, with further models and parameters. So far, KMeans and some limited similarity clustering techniques have been performed.
* Perform the first computational comparison between the Vipassana Institute and the Pali Text Society's editions of the Pali canon.


Project status:
* Data engineering - I have made a multi-zone data-model, which processes the data to be ingested for machine learning.


There are still some English language words passing through to the final stage. I will correct this before I begin performing extensive clustering (initial clustering results, however, look promising).


Although this is a personal and academic project, the repository reflects some of my personal preferences for data and machine learning engineering:
* KISS - use object-oriented programming, distributed computing, and cloud computing where appropriate.
* With modern open source libraries, united by pyarrow, high-performance python is possible on a single machine.
* Open source tools and inexpensive cloud services for horizontal and vertical scaling.
* Pre-commit and Makefiles for ease of setup and quality control (some of this is a little messy, and when working on other projects I would usually use CI/CD)

The data for this project all originates in open-source.
