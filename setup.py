import setuptools
import os

requirements = [
    i.strip() for i in open(os.path.join("requirements", "base.txt")).readlines()
]

setuptools.setup(
    name="guido",
    version="0.1",
    packages=["guido"],
    entry_points={"console_scripts": ["guido=guido.__main__:main"]},
    install_requires=requirements,
)
