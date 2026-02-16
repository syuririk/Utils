from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

with open(here / "requirements.txt") as f:
    requirements = f.read().splitlines()

package_names = ["getData", "processData"]
package_path = []
for n in package_names:
    package_path.append(n + ".*")


setup(
    name="utils",
    version="0.2.0",
    packages=find_packages(include= package_names + package_path), 
    include_package_data=True,
    install_requires=requirements,
    description="utils",
    long_description=(here / "README.md").read_text(encoding="utf-8") if (here / "README.md").exists() else "",
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
)