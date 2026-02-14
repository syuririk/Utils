import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="get_data", 
    version="1.0.0",
    author="syuririk",
    description="get data from apis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/syuririk/get_data",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        're',
        'requests'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)