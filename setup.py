from setuptools import setup, find_packages

setup(
    name="marketinsights-collector",
    version="0.1.0",
    description="Data collectors for economic and financial indicators",
    author="Econometrics Pipeline",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "psycopg2-binary>=2.9.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)