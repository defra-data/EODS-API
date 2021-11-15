import pathlib
from setuptools import setup, find_packages

VERSION = '1.0.0' 
DESCRIPTION = 'A wrapper library for pan-DEFRA users of the Earth Observation Data Service'
HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="eodslib", 
        version=VERSION,
        author="CGI Inc.",
        description=DESCRIPTION,
        long_description=README,
        long_description_content_type="text/markdown",
        license="MIT",
        packages=find_packages(),
        include_package_data=True,
        install_requires=["pandas==1.2.4",
                          "matplotlib==3.4.1",
                          "notebook==6.3.0",
                          "rasterio==1.2.3",
                          "requests==2.25.1",
                          "Shapely==1.7.1",
                          "pyproj==3.0.1",
                          "xmltodict==0.12.0",
                          "python-dotenv==0.17.1",
                          "pytest==6.2.4",
                          "pytest-mock==3.6.1",
                          "responses==0.13.4",
                          ],
        
        keywords=['python'],
        classifiers= [
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)
