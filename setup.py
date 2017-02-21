from setuptools import setup

setup(name='crawldb',
      version='0.7.15',
      description='a s3 and mongodb based database to store crawler data',
      url='https://github.com/changun/miso_crawldb',
      author='Andy Hsieh',
      author_email='andy@askmiso.com',
      license='MIT',
      packages=['crawldb'],
      install_requires=[
         'boto3',
         'typing',
         'pymongo',
         'enum34',

      ],
       # these two libraries are required for testing 'moto', 'mongomock'
      zip_safe=False)
