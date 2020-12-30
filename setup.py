"""Package setup file for restic-exporter."""

import setuptools

with open("README.md") as fh:
    long_description = fh.read()

setuptools.setup(
    name="restic-exporter",
    version="0.0.2",
    author="Dermot Duffy",
    author_email="dermot.duffy@gmail.com",
    description="Statistic exporter for restic backups",
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="restic backup statistics",
    url="https://github.com/dermotduffy/restic-exporter",
    package_data={"restic-exporter": ["py.typed"]},
    packages=setuptools.find_packages(exclude=["tests", "tests.*"]),
    platforms="any",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Archiving :: Backup",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.7",
    zip_safe=False,  # Required for py.typed.
)
