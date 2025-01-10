** See Project documentation for more informations.

#####################
# INJECTION AND ETL #
#####################

# Content of the folder:

	- dati_centraline.csv (input file)
		Comma Separated Value file with raw data from vehicles.

	- Injector.py (microservice 1)
		Estimated Time Required: 02' 06'' *
		It works with working Kafka and ZooKeeper Docker Container instances. 
		It uses standard ports. 

	- ETL1.py (microservice 2)
		Estimated Time Required: 01' 42'' *
		It works with working Kafka and ZooKeeper Docker Container instances.
		It uses standard ports.

	- ETL2.py (microservice 3)
		Estimated Time Required: 07' 32'' *
		It works with working Kafka and ZooKeeper Docker Container instances.
		It pushes the dataflow into a working MongoDB localhost server.
		It uses standard ports.

	- ETL3.py (microservice 4)
		Estimated Time Required: 00' 44'' *
		It works with working Kafka and ZooKeeper Docker Container instances.
		It pushes the dataflow into a working Redis Docker Container instance.
		It uses standard ports.		

	- 0_exploratory_analysis.ipynb
		Jupyter Notebook for a demonstrative and explorative analysis of microservices' input and outputs data.
		It can be used to visualize input and outputs produced by ETL's microservices, and to produce plots assessing
		the correctness of the operations carried out by them. 
		IMPORTANT: 
			This file should be executed after, at least, one run of the entire ETL to populate both MongoDB and Redis 
			databases with data.

	- Output_Elaborated.csv
		Output extracted from the elaborated collection of MongoDB database. 
		It's produced running "0_exploratory_analysis.ipynb" notebook.

	- Output_Simple.csv
		Output extracted from the simple collection of MongoDB database.
		It's produced running "0_exploratory_analysis.ipynb" notebook.

	- start_vs_simple.png
		Histogram Plot of Odometer and LifeConsumption variables.
		It's used to assess the correctness of the data flow comparing the original dataset (centraline.csv) and
		the output of the simple collection.
		It's produced running "0_exploratory_analysis.ipynb" notebook.

	- start_vs_simple_vs_elab_deltaLifeConsumption.png
		Histogram Plot of DeltaLifeConsumption variable.
		It's used to assess the correctness of the data flow comparing the manually computed Delta from the original dataset 
		(centraline.csv) and the Deltas produced as outputs of both simple (manually calculated in the notebook) and elaborated collections.
		It's produced running "0_exploratory_analysis.ipynb" notebook.

	- start_vs_simple_vs_elab_deltaOdometer.png
		Histogram Plot of DeltaOdometer variable.
		It's used to assess the correctness of the data flow comparing the manually computed Delta from the original dataset 
		(centraline.csv) and the Deltas produced as outputs of both simple (manually calculated in the notebook) and elaborated collections.
		It's produced running "0_exploratory_analysis.ipynb" notebook.

	- start_vs_simple_vs_elab_timeSeries.png
		Time Series Plot of DeltaOdometer and DeltaLifeConsumption variables.
		It's used to assess the correctness of the data flow comparing the manually computed Delta from the original dataset 
		(centraline.csv) and the Deltas produced as outputs of both simple (manually calculated in the notebook) and elaborated collections.
		It's produced running "0_exploratory_analysis.ipynb" notebook.
	
	- requirements.txt
		Text file listing Python libraries used with their version to reproduce the working environment.


* The Estimated Time Required refers to a machine with the following hardware configuration:
	- OS: Windows10 Home
	- CPU: Intel Core i7
	- RAM: 16 GB
	- Disk: SSD
		# Estimated Total Time Required: about 12 minutes.