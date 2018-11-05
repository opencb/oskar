try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='oskar',
    version='0.1.0',
    description='',
    long_description='',
    packages=['pyoskar', 'pyoskar.spark'],
    url='https://github.com/opencb/oskar/tree/develop/oskar-spark/src/main/python',
    license='Apache Software License',
    author='',
    author_email='',
    keywords='opencb oskar spark hadoop bioinformatics genomic',
    install_requires=[
        'pip >= 7.1.2',
        'requests >= 2.7',
        # 'avro == 1.7.7',
        'pyspark >= 2.2.2'
    ]
)
