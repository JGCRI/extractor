import pkg_resources
"""Extractor - functionality for extracting and formatting data from GCAM databases

Common uses are to prepare data from a GCAM database for use by other models and tools.
"""

__version__ = pkg_resources.get_distribution('extractor').version

__all__ = ['to_demeter', 'landclass_redistribute']