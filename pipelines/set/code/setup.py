from setuptools import setup, find_packages

setup(
    name='set',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='set',
    install_requires=[
        'pyhocon==0.3.59',
        'prophecy-libs==1.0.2'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    tests_require=['pytest', 'pytest-html']
)
