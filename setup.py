from setuptools import setup, find_packages


def get_requirements():
    with open('requirements.txt') as f:
        return f.read().split()

setup(
    name='extractor',
    version='0.1.0',
    description='Extract data from a GCAM database and format for use by other GCAM ecosystem models.',
    url='https://github.com/JGCRI/extractor',
    packages=find_packages(),
    license='MIT',
    author='Chris R. Vernon',
    author_email='chris.vernon@pnnl.gov',
    install_requires=get_requirements(),
    dependency_links=['git+https://github.com/JGCRI/gcam_reader@master#egg=gcam_reader-0.5.0'],
    include_package_data=True
)