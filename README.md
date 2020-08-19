  
# code    
    
This repo holds a variety of tools in support of the GDA Score Project (General Data Anonymity Score Project).    
    
The code here is still very much alpha, and little effort has gone into making it easy for others to install and use.    
    
The primary language is Python, and requires Python3.7 or later. API documentation for some of the tools can be found at https://gda-score.github.io/.    
    
## To run    
    
#### Installing via `pip`:
   - step 0: Install prerequisites for "psycopg2" (https://www.psycopg.org/docs/install.html#build-prerequisites). For debian the command would be `apt-get install python3-dev libpq-dev`.
  
   - step 1: `$ pip install gda-score-code`
   
   - step 2: Request passwords and user names from contact@gda-score.org
   
   - step 3: if you would like to stick to default configuration then skip this step. otherwise try executing `$ gdascore_init` in your console  to modify the configuration.
   This initiation script can also set required environment variables 
   (step 3) automatically.
       
   - step 4: gdascore package needs the following environment variables as for database credentials
   to work with Diffix and anonymization schemes that use the underlying postgres database at db001.gda-score.org. They are:
        - `GDA_SCORE_DIFFIX_USER` : `<diffix_user>` 
        - `GDA_SCORE_DIFFIX_PASS` : `<diffix_password>` 
        - `GDA_SCORE_RAW_USER` : `<postgres_user>` 
        - `GDA_SCORE_RAW_PASS` : `<postgres_password>`
            
    
   - step 5: use import statements such as the following in your code (see examples in `attacks` and `utility` repos):    
      ```python    
      from gdascore.gdaAttack import gdaAttack
      from gdascore.gdaScore import gdaScores    
      from gdascore.gdaTools import setupGdaAttackParameters  
      from gdascore.gdaQuery import *   
      ```

## How to update package on pip
Please follow the steps below if you are a contributor and want to upload new release of the library on pip.
 1. open [pypi.org](https://pypi.org) and create a new account if you don't have one.
 2. send us your username to be added as a maintainer of the package on pypi that lets you upload a new release
 3. install "twine" and "wheel" using: `pip install twine wheel`
 4. clean build files if there is any by running: `python setup.py clean --all` and also delete any folder named `dist` as the setup will create that itself and you won't face vrsion confilict because of past builds anymore.
 5. update the version of package in the file `setup.py` and increase it. it is not possible to upload same version twice so remember to do that. `name` parameter should not change. For example:
``` 
setuptools.setup(  
    name="gda-score-code",  
    version="2.2.6", # increase it
    author="Paul Francis",
    ...
  )
  ```
  5. build the package: `python setup.py sdist bdist_wheel`
  6. upload to pypi: `twine upload dist/*`
  7. it will first ask for your username and password and then start uploading.

**note**: you can always first upload the package into test.pypi.org instead of the main one to try installing it yourself and then upload to the main pypi repository. should you need to do that please refer to : [https://packaging.python.org/guides/using-testpypi/](https://packaging.python.org/guides/using-testpypi/)


## Documentation
Documentations for this project are being generated automatically out of "docStrings" available in the code.
We use [pdoc3](https://pypi.org/project/pdoc3/) as a tool for that. Steps for updating documentation are as follows:
 1. install "pdoc3", "PyInquirer", and "pyfiglet": `pip install pdoc3 PyInquirer pyfiglet`
 2. populate the code with docStrings in python modules, classes, functions, etc.
 3. on your terminal **go to project root folder** and execute the command bellow:
 ```shell
 pdoc -o ./docs --force --html --template-dir ./docs/template --filter gdaScore,gdaAttack,gdaTools gdascore
 ```
 
  - `-o ./docs`: defines output directory
  - `--force`: overrides current files in output directory
  - `--html`: defines output format
  - `--template-dir ./docs/template`: defines custom template directory that is used to generate output
  - `--filter gdaScore,gdaAttack,gdaTools`: defines modules that has to be documented from the package. any other python file in the package will be ignored for documentation.
  - `gdascore`: defines the name of package (directory)
 
### How to exclude certain part of code from documentation
pdoc will by default include every piece of code that is "public" in the documentation. To avoid that, define a list on top of your module
and name it `__all__` and put the name of classes, functions, variables, etc. that you want to be documented in that list as strings.
for example:
```python
module.py


__all__ = ["increase"]

my_var = 0

def increase():
    """
    this function increases the value of 'my_var' by 1 and return it.
    """
    global my_var
    my_var += 1
    return my_var
```

**Note that defining `__all__` will override things that gets imported when you use `from module import *` later.
However, you can have access to anything when you import them specifically like `from module import my_var` as well as `import module`. It is generally recommended to avoid `from module import *`**

### How to modify master html template for documentation
To modify the template that is used by pdoc for generating html formatted documentation, simply edit files under `/docs/template`:
 - `credits.mako` will be included in the footer of every page.
 - `head.mako`   will be included in the `<head>/<head>` of every page. It is good for modifying css styles, title, etc. of the page.
 - `logo.mako` will be included in logo placeholder (or simply header) of the page.
 
In case you need to perform more modification to the template, please refer to [original pdoc documentation](https://pdoc3.github.io/pdoc/doc/pdoc/)
