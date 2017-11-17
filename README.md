# cvrdata
This is a Python3 package for extracting data from danish CVR registry from Danish Business Authority and inserting in an sql database.

Close to being ready - but hang on just a little bit.

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

or use

``pip install -r requirements.txt``

