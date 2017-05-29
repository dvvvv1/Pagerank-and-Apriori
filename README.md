## Pagerank and Apriori ##
## Info ##
Author Name: Puzhi Yao

Author Email: dvvvv1@hotmail.com

## Instructions ##
## Prerequisite ##
Make sure all Hadoop components and libraries being installed in your system before running our programs.

## Error Handler 01 ##
If your terminal shows the error message like 'output exists'

please remove the output folder and try it again.

## Error Handler 02 ##
If your terminal shows the error message like 'XXXXXX library missing'

please add external jar library into the classpath:

slf4j-simple-1.7.21.jar

htrace-core4.jar

## Exercise 1 ##

1. Open eclipse

2. Import our project from `~/PageRank`. (File->Import->General->Exisiting Project into Workspace)

3. Right click the project and select "Run as" to configure Java application.

4. Create a new launch configuration on Java Application and type "pageRank.PageRank" in Main Class at right window.

5. Switch to Arguments, and type "web-Google.txt output".

6. Click the button of "Run".

7. Check the output folde in the current path (e.g. "~/PageRank/output") 

8. The folder, `Result`, contains all iteration results for Exercise 1.


## Exercise 2 ##

1. Open eclipse

2. Import our project from "~/Apriori". (File->Import->General->Exisiting Project into Workspace)

3. Right click the project and select "Run as" to configure Java application.

4. Create a new launch configuration on Java Application and type "aprioriMapReduce.AprioriMapReduce" in Main Class at right window.

5. Switch to Arguments, and type "data/chess.txt output". (all input datasets are stored in folder: data)

6. Click the button of "Run".

7. Check the output folde in the current path (e.g. "~/Apriori/output") 

## Instruction END ##
