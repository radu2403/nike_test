#This is the documentation of the Nike test.

## Introduction

The initial file structure was rearranged so that it looks and feels like a proper project.
The main folders are:

    *   data: where the CSV files lie
    *   documentation: where the exercise was described
    *   local_spark: an attempt to create with docker compose a cluster where the jobs could be send to process. 
    This should have mimic a deployment to AWS
    *   notebooks: a docker cluster was created and with the Jupyter Notebooks the data was investigated and solved
    *   src: where the code lies
    
## Code commentary

A pipeline architecture was created that build step-by-step the Spark transformations
It was done with a modified version of the Builder Pattern so that it can be easley constructed.

The transformations follow a ingest(clean) - from what I explored no cleaning was needed - in the future 
this should be implemented. Then followed by the transformations(simple and joinings).
The last part was the Sink (write to file)

## The development process

The development process was in 3 stages:

*   The Jupyter Notebook was created
*   Reordering and implementation of the rough code
*   Refactoring and documentation

# Running the code

The code will be ran just by a simple start at the main.scala file. 
There is the object Program that will trigger it.

# Future work

Use parameters to start the program.
Create a cluster to send the job.
Create a building system with deployment.
In a normal development environment all the classes would have been tested (future work).
The UDFs were tested in the notebook and the code was superficially tested there.

