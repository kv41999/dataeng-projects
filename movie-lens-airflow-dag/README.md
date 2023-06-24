This project demonstrates the following:
1. How to fetch data from an API.
2. The use of various airflow base classes for hooks, operators and sensors to create your own custom hooks, operators and sensors.

The **app.py** file mimics an online API from where we will fetch the data.
The three files named **hooks.py, operators.py and sensors.py** are the custom airflow classes created by making use of various base classes.
Finally the movie-lens-dag.py is the DAG file that will be used to schedule the tasks in airflow.
