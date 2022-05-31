from setuptools import setup, find_packages

setup(
    name='load_person_bronze',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='load_person_bronze',
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
