"""A setuptools based setup module"""

from setuptools import setup
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(

    name="comradewolf",

    version="0.0.2a1",

    description="Core to make OLAP-like engine. Creates SQL queries based on imported meta-data",

    long_description=long_description,

    long_description_content_type="text/markdown",
    url="https://github.com/konstantin-suspitsyn/comradewolf",

    author="Konstantin Suspitsyn",

    author_email="konstantin.suspitsyn@xmail.ru",

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Code Generators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="sql, code-generator",

    package_dir={"comradewolf": r"src/comradewolf",
                 "comradewolf.universe": r"src/comradewolf/universe",
                 "comradewolf.utils": r"src/comradewolf/utils", },

    packages=["comradewolf",
              "comradewolf.universe",
              "comradewolf.utils",],

    install_requires=["toml>=0.10.2",
                      "typing_extensions==4.10.0",],

    python_requires=">=3.10, <4",

    project_urls={
        "Bug Reports": "https://github.com/konstantin-suspitsyn/comradewolf/issues",
        "Source": "https://github.com/konstantin-suspitsyn/comradewolf",
    },
)