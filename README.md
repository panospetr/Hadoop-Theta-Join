#Hadoop Theta Join Implementation
MSC Course Project

An implementation of "Okcan A. et al., "Processing theta‐joins using MapReduce", SIGMOD, 2011" with the improvements proposed at "Koumarelas I. et al., "Binary Theta-Joins using MapReduce: Efficiency Analysis and Improvements", EDBT/ICDT, 2014"

##command line execution:

hadoop jar <Path-to-project-jar/HadoopProject.jar> <DFS-InputFilePATH > <DFS-
OutPutPATH> <DFS-TempFilePATH> |S| |R| partitionNumber

where:
<Path-to-project-jar/HadoopProject.jar> is the jar location
<DFS-InputFilePATH > is the input file lcoation in DFS
<DFS-OutPutPATH> is the output file location in DFS
<DFS-TempFilePATH> is the location of an intermediate file in DFS for saving the results of the first reduce cycle
|S| number of tuples for S
|R| number of tuples for R
partitionNumber number of partitions



