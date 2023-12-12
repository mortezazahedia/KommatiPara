# KommatiPara

I used XML-style comments just for my package 

## for creating distribution package you have to run  
Runing below command
```commandline
pip3 install . 
or
pip3 install --upgrade .
```
in the same Directory with setup.py 
  

## to run main.py you can use below commands
```commandline
python .\src\main.py .\input_files\dataset_one.csv .\input_files\dataset_two.csv Netherlands
python .\src\main.py .\input_files\dataset_one.csv .\input_files\dataset_two.csv 'Netherlands,United Kingdom'
```

## for creating code coverage I createed Makefile
you can run it in command line with below command
```commandline
	pytest --cov-report html --cov=src Test
```


NOTE:
I have doubt about PII and I implement remove_pii() but it depends on BA's order