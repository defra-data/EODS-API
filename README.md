# EODS-API
A collection of example scripts for Defra Core and ALB users of the Earth Observation Data Service. You can find example videos on how to use these scripts at https://earthobs.defra.gov.uk/help/


# Installing Miniconda and launching Jupyter notebook:

1.	If you do not already have it, download and install Miniconda on your PC/laptop: https://docs.conda.io/en/latest/miniconda.html 
2.	Save the environment.yml file provided by JNCC in the folder on your PC/laptop which contains the Miniconda files.
3.	Save the three *.ipynb files, the config.py file and the xml folder provided by JNCC in the sub-folder called Scripts within the folder on your PC/laptop which contains the Miniconda files.
4.	Open the Anaconda Powershell Prompt – this should now be available on your start menu.  Navigate to this directory using the Anaconda Powershell Prompt using the following command:  
cd <filepath>
5.	Create the environment using the following command:
conda env create --file environment.yml python=3.6
6.	Activate the environment using the following command: 
conda activate eods
7.	Launch the Jupyter Notebook in your browser with the following command:
jupyter notebook
8.	The Jupyter Notebook should now be open in your web browser, enabling you to view and navigate the contents of your Miniconda directory.  Open the Scripts folder.  
9.	Open the config file from the Scripts folder and add your user name and authentication token.
10.	Open one of the notebooks from the Scripts folder, e.g. start with ‘simple-query’.
