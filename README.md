# KommatiPara
[![Build](https://github.com/mortezazahedia/KommatiPara/actions/workflows/main.yaml/badge.svg)](https://github.com/mortezazahedia/KommatiPara/actions/workflows/main.yaml)

This project get to Dataframe and joined them wiyh eachother on id column then remove PII information and store the result in csv/parquet format

## for creating distribution package you can run  
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
	pytest --cov-report html --cov=tests
```

## for using flake8
you can run
```commandline
	flake8 .
```

> for creating requirements.txt I use
 ```
 pip freeze requirements.txt
 ```

NOTE:
- I have doubt about PII and I implement remove_pii() but it depends on BA's order
- I used XML-style comments just for my package 
