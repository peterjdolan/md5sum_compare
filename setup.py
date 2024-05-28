from setuptools import setup, find_packages

setup(
    name="md5sum_compare",
    version="0.1.0",
    author="Peter Dolan",
    author_email="peter@peterdolan.us",
    description="A script to generate and compare md5sum manifests of directories.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/peterjdolan/md5sum_compare",
    packages=find_packages(),
    install_requires=[
        "aiofiles",
        "pandas",
        "tqdm",
    ],
    entry_points={
        "console_scripts": [
            "md5sum_script=md5sum_script.script:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)