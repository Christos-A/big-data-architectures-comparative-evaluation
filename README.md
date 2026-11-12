# Comparative Evaluation of Modern Big Data Architectures

This repository contains the implementations, benchmark scripts, and experimental results supporting the research paper **"A Comparative Evaluation Framework for Modern Big Data Architectures."**

## Overview

The study investigates six modern Big Data architectural paradigms:

- Data Lake
- Data Lakehouse
- Data Mesh
- Data Fabric
- Semantic Data Fabric
- Knowledge Graph Lakehouse

The architectures are examined using eight evaluation dimensions covering architectural goals, data organization, metadata management, semantic support, governance, processing and integration, performance, and scalability.

The objective is to provide a structured comparison of architectural capabilities, implementation characteristics, and trade-offs, rather than identifying a universally superior architecture.

## Implementation

The experimental environment uses Docker, MinIO, Hive Metastore, Trino, Apache Iceberg, and Python to represent and evaluate the six architectural approaches.

Historical weather observations from Cyprus, covering 2020–2024, are used as the analytical dataset.

The implementation scripts support data ingestion, transformation, architectural checks, and query benchmarking.

## Experimental Evaluation

Field 6 examines processing and integration through implementation-oriented checks.

Field 7 evaluates query performance using five analytical queries of increasing complexity. Each query is executed five times, with the first execution excluded as warm-up.

The repository includes benchmark execution details, query-level summaries, and architecture-level results.

## Reproducibility

The repository provides the available research scripts and selected experimental evidence. A fully automated reproduction from a clean environment has not yet been verified, and some infrastructure initialization scripts remain to be added.

The included configuration examples require review before deployment.

## Research Paper

This repository accompanies the paper *A Comparative Evaluation Framework for Modern Big Data Architectures*.

Publication details and the final citation will be added when available.
