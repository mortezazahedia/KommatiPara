import setuptools

setuptools.setup(
    name='KommatiPara_Pkg',
    version='0.0.1',
    description='Create package for my assignment',
    url='https://github.com/mortezazahedia/KommatiPara',
    author='Morteza Zahedi Anaraki',
    author_email='morteza.zahedi.a@gmail.com',
    packages=setuptools.find_packages(exclude=['tests', 'src.main']),
    install_requires=['pyspark>=3.0.0', 'pytest'],
    long_description='Create package for my assignment',
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
         "Operating System :: OS Independent",
    ],
)
