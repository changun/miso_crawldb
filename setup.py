from setuptools import setup

setup(name='crawldb',
      version='0.6.4',
      description='a dyanamo-based db to store crawler data',
      url='https://github.com/changun/miso_crawldb',
      author='Andy Hsieh',
      author_email='andy@askmiso.com',
      license='MIT',
      packages=['crawldb'],
      install_requires=[
          'boto3',
          'enum34'
      ],
      zip_safe=False)