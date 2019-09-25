
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
	   from gdascore.gdaScore import gdaAttack, gdaScores  
	   from gdascore.gdaUtilities import *
	   from gdascore.gdaQuery import *
	   ```  

