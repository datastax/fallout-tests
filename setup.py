"""setup tool"""
from setuptools import find_packages, setup

setup(
    name='hunter_csv',
    version='0.1.0',
    test_suite='tests',
    packages=find_packages(include=['src']),
    install_requires=[
        "autopep8==1.6.0",
        "boto3==1.24.28",
        "isort==5.9.3",
        "pandas==1.4.2",
        "pytest==7.2.2",
        "pytest-cov==4.0.0",
        "pip==23.0.1"
    ],
    python_requires='>=3.9.2'
)
