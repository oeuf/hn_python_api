from setuptools import setup, find_packages
setup(
    name = "hacker_news_api_client",
    version = "0.0.1",
    packages = find_packages(),
    author='Nick Gustafson',
    author_email='njgustafson@gmail.com',
    url='https://github.com/oeuf/hacker_news_api_client',
    description='scratchpad code for accessing data from Hacker News api',
    install_requires=[
        'psycopg2',
        'requests',
    ],
)
