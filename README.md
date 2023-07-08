# Data Engineering Pipeline with Apache Airflow

Welcome to the repository for my Data Engineering pipeline implemented with Apache Airflow! This project showcases my skills and expertise in designing and building efficient data pipelines. Here, I'll provide you with an overview of the project and guide you through its setup and execution.

## Project Overview

The main objective of this project is to demonstrate the successful implementation of a data pipeline using Apache Airflow. The pipeline performs the following tasks:

1. Extraction: The pipeline reads data from Elastic, specifically documents that are missing certain parameters.

2. Transfer: To fill in those missing parameters, an AI endpoint is called through an API to obtain the required information.

3. Loading: The processed data, enriched with AI tags, is published into Kafka, ensuring seamless continuation of the pipeline.

To optimize the performance and overcome potential bottlenecks in the transfer phase, I implemented multithreading and batching techniques. This approach significantly reduces latency by converting data into bulk arrays and making simultaneous API calls.

The project utilizes two Directed Acyclic Graphs (DAGs):

1. `multithreading_pipeline.py`: Implements the pipeline using Python's multithreading package to enhance efficiency and parallelism.

2. `dynamic_task_mapping_pipeline.py`: Demonstrates the usage of Airflow's dynamic task mapping method called "expand" to achieve scalability and flexibility.

The pipeline is containerized using Docker, making it easy to deploy and manage across different environments.

## Installation and Setup

To get started with this project, follow these steps:

1. Clone the repository to your local machine using the command:


    `git clone https://github.com/saeedmzr/airflow_elastic_kafka.git`


2. Ensure that you have Docker installed on your system. If not, download and install Docker from [https://www.docker.com](https://www.docker.com).

3. Navigate to the project directory and launch the Docker containers by running:

    `docker-compose up -d`

    This command will start all the required services, including Airflow, Kafka, Postgres, and others.

4. Access the Airflow UI by opening a web browser and entering `http://localhost:8080`. You can use the default credentials: `username=airflow` and `password=airflow`.

5. In the Airflow UI, enable the desired DAG(s) by toggling the corresponding switch(es). This will trigger the pipeline execution according to the specified schedule or trigger.

## Repository Structure

The repository is structured as follows:
<pre>
data-engineering-pipeline/
├── dags/
│   ├──tasks/
│   │    ├──/emotions.py
│   ├──components/
│   │    ├──/elastic.py
│   │    ├──/kafka.py
│   ├── multi_threading.py
│   ├── expand.py
│   ├── setting.py
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── README.md
</pre>

- The `dags/` directory contains the two DAG files:
  - `multi_threading.py`: Implements the data pipeline using Python's multithreading package.
  - `expand.py`: Demonstrates the usage of Airflow's dynamic task mapping method.

- The `docker-compose.yml` file defines the services required for the project, including Airflow, Kafka, Postgres, Redis, and others.

## Conclusion

This repository showcases my skills as a Data Engineer and my ability to design and build efficient data pipelines. Through the utilization of Apache Airflow, Docker, and various optimization techniques, I have created a robust pipeline capable of handling data extraction, transformation, and loading tasks.

Feel free to explore the code, experiment with different configurations, and adapt the pipeline to your specific use cases. If you have any questions or feedback, please don't hesitate to reach out. Let's connect and explore new opportunities in the world of Data Engineering!

