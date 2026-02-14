from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8") if (here / "README.md").exists() else ""

with open(here / "requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="get_data",
    version="0.1.0",
    packages=find_packages(include=["funcs", "funcs.*"]),  # funcs 포함
    include_package_data=True,
    install_requires=requirements,
    description="Get data from ECOS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
)