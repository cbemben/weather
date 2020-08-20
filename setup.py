import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="thunderclap-pancake", # Replace with your own username
    version="0.0.1",
    author="Christopher Bemben",
    author_email="cbemben04@gmail.com",
    description="Build historical weather data for specific US zip codes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cbemben/weather",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)