"""Extractor - functionality for extracting and formatting data from GCAM databases

Common uses are to prepare data from a GCAM database for use by other models and tools.
"""
import pkg_resources

from extractor.to_demeter import GcamToDemeter
from extractor.demeter_landclass_split import GcamLandclassSplit


__version__ = pkg_resources.get_distribution('extractor').version

__all__ = ['to_demeter', 'demeter_landclass_split']