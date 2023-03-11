# cvrdata
This is a Python3 package for extracting data from danish CVR registry from Danish Business Authority and inserting in an sql database.


You need to get username and password from the Danish Business Authority to access the data on their servers, alas to use this package for anything.


If you use this script to fetch data for academic purposes please cite it as follows (BibTeX):
```bibtex
@misc{erst_data,
  author = {Allan Gr{\o}nlund and Jesper Sindahl Nielsen},
  title = {Danish {B}usiness {A}uthority {D}ata {F}etcher},
  year = {2017},
  howpublished = {\url{https://github.com/jasn/regnskaber}}
}
```

# Setup
The module can be installed using pip after installing the dependencies mentioned earlier.
Run the following command:

``pip install git+https://github.com/gronlund/cvrdata``

*Only works with elasticsearch 6*

#  Command Line Interface
To use the program from the cmd line use

``python -m cvrparser <command> ``

## Configuration
To setup database and username and password to use to download from Danish Business Authority servers use

``python -m cvrparser reconfigure``

## Database Tools
To create the database tables use

``python -m cvrparser dbsetup -t``

## Get Data
To update the cvr database run

``python -m cvrparser update ``

Notice that running from an empty database is faster than updating and existing but old version of the data.

To insert DBA registrations run

``python -m cvrparser get_regs ``
