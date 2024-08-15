import setuptools
import os

requirements = [
    i.strip() for i in open(os.path.join("requirements", "base.txt")).readlines()
]
long_description = open("README.md").read()

setuptools.setup(
    name="guido-kafka",
    version="0.1",
    packages=setuptools.find_packages(),
    entry_points={"console_scripts": ["guido=guido.__main__:main"]},
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type="text/markdown"
)
