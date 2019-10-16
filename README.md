  
# code    
    
This repo holds a variety of tools in support of the GDA Score Project (General Data Anonymity Score Project).    
    
The code here is still very much alpha, and little effort has gone into making it easy for others to install and use.    
    
The primary language is Python, and requires Python3.7 or later. API documentation for some of the tools can be found at https://gda-score.github.io/.    
    
## To run    
    
#### Installing via `pip`:    
   - step 1: `$ pip install gda-score-code`    
            
   - step 2: if you would like to stick to default configuration then skip this step. otherwise try executing `$ gdascore_init` in your console  to modify the configuration.    
    
   - step 3: use import statements such as the following in your code (see examples in `attacks` and `utility` repos):    
      ```python    
      from gdascore.gdaAttack import gdaAttack
      from gdascore.gdaScore import gdaScores    
      from gdascore.gdaTools import *  
      from gdascore.gdaQuery import *  
      ```

## How to update package on pip
Please follow the steps below if you are a contributor and want to upload new release of the library on pip.
 1. open [pypi.org](https://pypi.org) and create a new account if you don't have.
 2. send us your username to be added as a maintainer of the package on pypi that lets you upload a new release
 3. install `twine` using: `pip install twine`
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
