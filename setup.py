from setuptools import setup

setup(
    name='cvrparser',
    version=0.1,
    url='https://github.com/gronlund/cvrdata',
    description=('A module for fetching Central Business Register data from the Danish Business Authority'),
    author='Allan Gronlund',
    author_email='allan.g.joergensen@gmail.com',
    license='MIT',
    packages=['cvrparser'],
    install_requires=[
        'SQLAlchemy>=1.1.14',
        'chardet>=3.0.4',
        'elasticsearch',
        'elasticsearch-dsl',
        'idna>=2.6',
        'mysqlclient>=1.3.12',
        'numpy>=1.13.3',
        'python-dateutil>=2.6.1',
        'python_Levenshtein>=0.12.0',
        'pytz>=2017.3',
        'six>=1.11.0',
        'requests',
        'tqdm>=4.19.4',
        'ujson>=1.35',
        'urllib3>=1.22'
    ],
)