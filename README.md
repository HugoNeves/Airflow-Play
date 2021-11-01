# Prerequisites
 - Docker installed and running.
 - 10 GB of free disk space for Docker to use. Mainly because of Airflow.

# How to run the project
1. Start from the root directory.
2. Create the docker network that will have all the services running.
   - `docker network create local_network`
3. Start the provisioned Postgres container. It will automatically create the schemas and the tables. This will act as a source database but also as the data warehouse.
   - `docker-compose up postgres-dwh -d`
4. Start the Python container that will generate the random source data. This includes the B2B and the access_log data.
   - `docker-compose up generate-data-sources --build --force-recreate`
5. Initialize Airflow. This will take some minutes and will exit correctly by itself with code 0.
   - `docker-compose -f airflow.docker-compose.yaml up airflow-init`
6. When the previous initialization is finished, start all the services related to Airflow. This also takes a while.
   - `docker-compose -f airflow.docker-compose.yaml up`
7. When the above is ready, the Web UI will be available at http://localhost:8080. Credentials are `airflow`/`airflow`.
8. There are 3 DAGs created: one for the load of the **Dimensions**, one for the load of the **Facts**, and another one to do the **full load**, which is basically the Dimensions followed by the Facts.
9. When running the DAGs, be aware that the respective toggle (on the left) has to be active. If not, they will not start.

## How to delete everything and start from scrath
1. `docker-compose -f airflow.docker-compose.yaml down --volumes --remove-orphans`
2. `docker network remove local_network`
