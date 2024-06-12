
##  annalect
annalect interview project contains all the required modules 

## Installation

build docker image that contains stand alone single node cluster : `docker-compose up -d`

spark ui is available at : `http://localhost:8080/`

find spark master container name using: `docker ps -a`

run script that reads csv file and outputs stats by submitting job to the cluster
`docker exec -it annalect-spark-master-1  spark-submit --master spark://annalect-spark-master-1:7077 /app/crude_oil.py`

above command will display output for three questions in the interview request
(screenshots of sample run are provided below)

stop all running containers: `docker-compose down`


## Screenshots that contains output

![screenshot](output.png)

spark UI screenshot that contains submitted application

![screenshot](spark_ui.png)
