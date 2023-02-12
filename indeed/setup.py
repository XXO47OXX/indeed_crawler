# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'project',
    version      = '1.0',
    packages     = find_packages(),
    scripts      = ['main_scraper_1.py'],
    package_data = {'indeed': ['*.json']},
    entry_points = {'scrapy': ['settings = indeed.settings']},
    zip_safe=False,
)
