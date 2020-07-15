# -*- coding: utf-8 -*-
"""
test_database_json
~~~~~~~~~~~~~

Test the JSON database backend.

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import logging
import unittest
import os

from chemdataextractor.model import MeltingPoint, Compound, ModelList

from cdedatabase import CDEDatabase, JSONCoder

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


def remove_database(db_name):
    for f_name in os.listdir(db_name):
        os.remove(os.path.join(db_name, f_name))
    os.rmdir(db_name)


class TestJSONDatabase(unittest.TestCase):

    def setUp(self):
        self.db_name = 'TestDatabase'

    def test_add(self):
        db = CDEDatabase(db_name=self.db_name,
                         coder=JSONCoder())
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        db.add(mps)
        results = []
        for result in db.records(MeltingPoint):
            results.append(result.serialize())
        serialized = mps.serialize()
        self.assertCountEqual(results, serialized)
        remove_database(self.db_name)

    def test_update(self):
        db = CDEDatabase(db_name=self.db_name,
                         coder=JSONCoder())
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        db.add(mps)
        mp_new = MeltingPoint(raw_value='120', raw_units='K',
                              compound=Compound(names=['Water']))
        mp_new._id = mp._id
        db.update([mp_new])
        mps = ModelList(mp_new, mp_2, mp_3)
        serialized = mps.serialize()
        results = []
        for result in db.records(MeltingPoint):
            results.append(result.serialize())
        self.assertCountEqual(results, serialized)
        remove_database(self.db_name)

    def test_delete(self):
        db = CDEDatabase(db_name=self.db_name,
                         coder=JSONCoder())
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        db.add(mps)
        db.delete(MeltingPoint, [mp._id])
        results = []
        for result in db.records(MeltingPoint):
            results.append(result.serialize())
        mps = ModelList(mp_2, mp_3)
        serialized = mps.serialize()
        self.maxDiff = -1
        self.assertCountEqual(results, serialized)
        remove_database(self.db_name)

    def test_record(self):
        db = CDEDatabase(db_name=self.db_name,
                         coder=JSONCoder())
        mp = MeltingPoint(raw_value='200', raw_units='K',
                          compound=Compound(names=['H2O']))
        mp_2 = MeltingPoint(raw_value='150', raw_units='K',
                            compound=Compound(names=['CH4']))
        mp_3 = MeltingPoint(raw_value='600', raw_units='K',
                            compound=Compound(names=['benzene']))
        mps = ModelList(mp, mp_2, mp_3)
        db.add(mps)
        retrieved_mp = db.record(MeltingPoint, mp._id)
        self.assertEqual(retrieved_mp.serialize(), mp.serialize())
        remove_database(self.db_name)

