import setuptools

setuptools.setup(
    name="guido",
    version="0.1",
    packages=["guido"],
    entry_points={"console_scripts": ["guido=guido.__main__:main"]},
)
