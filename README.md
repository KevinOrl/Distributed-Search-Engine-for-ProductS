Distributed Search Engine for Products

This project implements a distributed system designed to search and analyze products from online stores such as Amazon and eBay, simulating real user behavior. The system automates data collection, storage, processing, and visualization through a microservices architecture deployed with Docker containers and Kubernetes orchestration.

Purpose

To automate the extraction of product information (price, description, images, etc.) from online stores, process it, and enable efficient search and analysis via Kibana.

System Architecture

The system consists of several services that communicate via RabbitMQ and manage document states using MariaDB:
- Selenium + Boto3 App: Simulates user navigation, extracts product HTML, and stores it in an S3 bucket.
- S3 Spider: Detects new or modified documents, updates their state in MariaDB, and publishes events to RabbitMQ.
- Downloader: Retrieves documents from S3, indexes them in Elasticsearch, and updates their status.
- Processor: Extracts relevant information using Beautiful Soup and publishes it to a dedicated Elasticsearch index.
- Kibana: Enables end users to perform searches and visualize product data.

Technologies Used

- Language: Python
- Scraping: Selenium, Beautiful Soup
- Cloud Storage: Amazon S3 (via Boto3)
- Messaging: RabbitMQ
- Databases: MariaDB, Elasticsearch
- Visualization: Kibana
- Containers: Docker
- Orchestration: Kubernetes + Helm Charts

Technical Challenges

- Coordinating microservices in a distributed environment.
- Detecting changes in HTML documents using MD5 hashing.
- Managing real-time error handling and document states.
- Automating deployment with Docker and Kubernetes.

Project Status

Functional academic project focused on distributed architecture, automation, and data processing. Suitable as a foundation for price monitoring systems or product analysis platforms.
