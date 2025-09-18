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
    extras_require={
        "selenium": [
            "scipy>=1.11.0",
            "selenium>=4.15.0",
            "webdriver-manager>=4.0.0",
        ],
        "investiny": [
            "investiny @ git+https://github.com/cwilko/investiny.git@feature/curl-cffi-support",
        ],
    },
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