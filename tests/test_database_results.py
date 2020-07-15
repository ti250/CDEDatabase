# -*- coding: utf-8 -*-
"""
test_database_results
~~~~~~~~~~~~~

Test the Results class in the database module.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import unittest

from chemdataextractor.model import MeltingPoint, Compound, ModelList

from cdedatabase import CDEDatabase, JSONCoder
from cdedatabase.results import Results

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

class TestResults(unittest.TestCase):

    def test_filtered(self):
        def filter_func(element):
            if element.value[0] > 160:
                return True
            return False
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        filtered = Results(mps, None).filtered(filter_func).all()
        expected = [mp, mp_3]
        self.assertCountEqual(filtered, expected)

    def test_sorted(self):
        def sort_key(element):
            return element.value[0]
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        sorted_list = Results(mps, None).sorted(sort_key).all()
        expected = [mp_2, mp, mp_3]
        self.assertCountEqual(sorted_list, expected)
