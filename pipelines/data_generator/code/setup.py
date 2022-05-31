from setuptools import setup, find_packages

setup(
    name='data_generator',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='data_generator',
    install_requires=[
        'pyhocon==0.3.59',
        'prophecy-libs==1.0.0'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    tests_require=['pytest', 'pytest-html']
)
