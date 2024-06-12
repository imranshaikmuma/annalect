
##  annalect
annalect interview project contains all the required modules 

## Installation

build docker image: docker build -t my-pyspark-image .



## Directory Structure
- `core`: Contains core classes for common tasks like opening and reading a file and calculate common tasks like counting number of employees
- `tests`: Contains test classes for testing various modules like ingesting data, processing etc 
- `inputs`: Contains various input combinations
- `employee_pairs.py`: Contains main script that performs all core functions and outputs the end result of employee pairs
- `persons.json`: Contains the input JSON file that contains employee experiences


## Execution
cd to the project directory and call the code using 

`python3 employee_pairs.py`

## Testing
Tests for the framework and product-specific pipelines can be found within the tests directories in the corresponding subdirectories. The framework code uses pytest or similar testing library, while product-specific pipeline tests use great expectations library to test the data.

For running tests in `tests/transformation`, run
``` bash
pytest tests/transformation
```
