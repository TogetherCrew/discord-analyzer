from setuptools import find_packages, setup

with open("requirements.txt") as f:
    requirements = f.read().splitlines()


setup(
    name="tc-analyzer-lib",
    version="1.0.0",
    author="Mohammad Amin Dadgar, TogetherCrew",
    maintainer="Mohammad Amin Dadgar",
    maintainer_email="dadgaramin96@gmail.com",
    packages=find_packages(),
    description="A platform agnostic analyzer, computing the TogetherCrew dashboard metrics.",
    long_description=open("README.md").read(),
    install_requires=requirements,
)
