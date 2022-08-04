# Blacklisting
## Folder Structure
```
api/                        [FastAPI, responsible for exposing data to other services]
data/                       [Contains smaller datasets suitable for GitHub storage]
db/                         [SQLAlchemy ORM, contains SQL stuff (models, schemas)]
experimental_notebooks/     [Messy notebooks used for testing ideas]
notebooks/                  [More organized notebooks, should be functional and understandable]
scripts/                    [Executable directly from a terminal, obvious purposes]
src/                        [Contains the actual blacklisting algorithms]
utils/                      [Helpers mostly related to storage and data loading]
```
## Installation
### Add directory to PYTHONPATH system variable
#### Windows
Create a new system variable named ```PYTHONPATH``` with value ```FULL PATH TO THIS REPOSITORY (e.g. C:\Users\steve\Development\blacklisting\)```
#### Linux
```export PYTHONPATH=${PYTHONPATH}:$(pwd)```
### Google Cloud Services
For files related to any form of Google Cloud Service, you need a system variable named ```GOOGLE_APPLICATION_CREDENTIALS``` pointing to a Google Cloud credentials json-file.
