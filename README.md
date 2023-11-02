# Tipitaka NLP Project

During 2021-2022, I pursued Buddhist Studies at the University of Oxford. Initially, I aimed to perform clustering on the entirety of the Pali canon. However, my interest shifted more towards data and software engineering than academic study of Classical Chinese and Pali. Now, I've returned to this project, eager to perform NLP on the expansive Pali canon, which is roughly three times the length of the King James Bible.

## Background

This project draws inspiration from:
- **Dan Zigmond**, who undertook the first computational analysis of the Pali canon using R.
  - [Read the paper](http://jocbs.org/index.php/jocbs/article/view/236)
  - ⚠️ Note: Use the above link at your own risk; the website lacks HTTPS security.
- **The Pali Text Society**, which has diligently upheld the academic standard editions of the Pali canon.

## Project Objectives

1. Replicate Dan Zigmond's results, but using Python.
2. Conduct a more comprehensive clustering, exploring various models and parameters. Current techniques involve KMeans and a few basic similarity clustering techniques.
3. Undertake the first computational comparison between the Vipassana Institute's and the Pali Text Society's editions of the Pali canon.

## Current Progress

- **Data Engineering**: Developed a multi-zone data-model for preparing data for machine learning. Ingested both pali text society and vipassana research institute data.
- **Clustering**: Leveraging the open-source tool `Metaflow` in conjunction with AWS services (S3 & Batch).
  - Note: Cloning and replicating results may pose challenges due to this setup. The Terraform files won't be shared here, as I use the resources across various projects. However, modifications to wtanner's personal terraform files were minimal. Thanks to wtanner!
  - The potential for horizontal/vertical scaling and repeatable ML DAGs offered by Metaflow is substantial.
- **Result Documentation**: Upon completion, a comprehensive analysis of my findings will be provided, with hopes of future publication.

**Ongoing Tasks**:
- Replicating Dan Zigmond's clustering (KMeans and agglomerative clustering) results in python.

**Next Steps**:
1. Perform involved clustering on the Pali Text Society data.
2. Conduct a computational comparison of the Pali Text Society and Vipassana Edition Pali Canons.

## Personal Notes

Even though this is an academic project, the repository encapsulates some of my personal inclinations regarding data and machine learning engineering:
- **KISS Principle**: Implement OOP, distributed computing, intricate database schemas, and cloud computing _when necessary_.
- **High-Performance Python**: Utilizing contemporary open-source libraries and pyarrow, python's efficiency can be significantly enhanced on a single machine.
- **Scalability**: Leverage open-source tools and budget-friendly cloud services for both horizontal and vertical scaling.
- **Code Quality Control**: Use pre-commit and Makefiles for a streamlined setup and maintaining code quality. Although some sections might seem a bit cluttered, in professional projects, I typically opt for CI/CD.
- **Limited Use of Notebooks**: While Jupyter Notebooks excel for educational purposes and demonstrations, for repeatable code in result-driven projects, I prefer not to rely on them.

All data used in this project is sourced from open-source platforms.
