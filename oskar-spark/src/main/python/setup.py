try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='pyoskar',
    version='0.1.0',
    author='Ignacio Medina',
    author_email='im411@cam.ac.uk',
    description='Pyoskar is an open-source project that aims to implement a framework for genomic scale _big data_ analysis',
    url='https://github.com/opencb/oskar/tree/develop/oskar-spark/src/main/python',
    packages=['pyoskar', 'notebooks'],
    license='Apache Software License',
    keywords='opencb oskar pyoskar spark pyspark hadoop bioinformatics genomic',
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pip >= 7.1.2',
        'pyspark >= 2.2.2',
        'requests >= 2.7',
        # 'avro == 1.7.7',
        'numpy',
        'pandas'
    ]
)
