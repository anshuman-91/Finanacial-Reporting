from setuptools import setup, find_packages

setup(
    name='load_products_bronze',
    version='1.0',
    packages=find_packages(include=('job*',)),
    description='load_products_bronze',
    install_requires=[
        'pyhocon==0.3.59',
        'prophecy-libs==0.1.6'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    tests_require=['pytest', 'pytest-html']
)
