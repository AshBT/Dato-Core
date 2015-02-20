# -*- coding: utf-8 -*-
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
from graphlab.data_structures.sarray import SArray
from graphlab_util.timezone import GMT
import pandas as pd
import numpy as np
import unittest
import random
import datetime as dt
import copy
import os
import math
import shutil
import array
import util
import time
import itertools
import warnings

#######################################################
# Metrics tracking tests are in test_usage_metrics.py #
#######################################################

class SArrayTest(unittest.TestCase):
    def setUp(self):
        self.int_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.bool_data = [x % 2 == 0 for x in range(10)]
        self.datetime_data = [dt.datetime(2013, 5, 7, 10, 4, 10),dt.datetime(1902, 10, 21, 10, 34, 10),None]
        self.float_data = [1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]
        self.string_data = ["abc", "def", "hello", "world", "pika", "chu", "hello", "world"]
        self.vec_data = [array.array('d', [i, i+1]) for i in self.int_data]
        self.list_data = [[i, str(i), i * 1.0] for i in self.int_data]
        self.dict_data =  [{str(i): i, i : float(i)} for i in self.int_data]
        self.url = "http://s3-us-west-2.amazonaws.com/testdatasets/a_to_z.txt.gz"

    def __test_equal(self, _sarray, _data, _type):
        self.assertEqual(_sarray.dtype(), _type)
        self.assertSequenceEqual(list(_sarray.head(_sarray.size())), _data)

    def __test_creation(self, data, dtype, expected):
        """
        Create sarray from data with dtype, and test it equals to
        expected.
        """
        s = SArray(data, dtype)
        self.__test_equal(s, expected, dtype)
        s = SArray(pd.Series(data), dtype)
        self.__test_equal(s, expected, dtype)

    def __test_creation_type_inference(self, data, expected_dtype, expected):
        """
        Create sarray from data with dtype, and test it equals to
        expected.
        """
        s = SArray(data)
        self.__test_equal(s, expected, expected_dtype)
        s = SArray(pd.Series(data))
        self.__test_equal(s, expected, expected_dtype)

    def test_creation(self):
        self.__test_creation(self.int_data, int, self.int_data)
        self.__test_creation(self.int_data, float, [float(x) for x in self.int_data])
        self.__test_creation(self.int_data, str, [str(x) for x in self.int_data])

        self.__test_creation(self.float_data, float, self.float_data)
        self.assertRaises(TypeError, self.__test_creation, [self.float_data, int])

        self.__test_creation(self.string_data, str, self.string_data)
        self.assertRaises(TypeError, self.__test_creation, [self.string_data, int])
        self.assertRaises(TypeError, self.__test_creation, [self.string_data, float])

        expected_output = [chr(x) for x in range(ord('a'), ord('a') + 26)]
        self.__test_equal(SArray(self.url, str), expected_output, str)

        self.__test_creation(self.vec_data, array.array, self.vec_data)
        self.__test_creation(self.list_data, list, self.list_data)

        self.__test_creation(self.dict_data, dict, self.dict_data)

        # test with type inference
        self.__test_creation_type_inference(self.int_data, int, self.int_data)
        self.__test_creation_type_inference(self.float_data, float, self.float_data)
        self.__test_creation_type_inference(self.bool_data, int, [int(x) for x in self.bool_data])
        self.__test_creation_type_inference(self.string_data, str, self.string_data)
        self.__test_creation_type_inference(self.vec_data, array.array, self.vec_data)
        self.__test_creation_type_inference([np.bool_(True),np.bool_(False)],int,[1,0])

    def test_list_with_none_creation(self):
        tlist=[[2,3,4],[5,6],[4,5,10,None]]
        g=SArray(tlist)
        self.assertEqual(len(g), len(tlist))
        for i in range(len(tlist)):
          self.assertEqual(g[i], tlist[i])

    def test_list_with_array_creation(self):
        import array
        t = array.array('d',[1.1,2,3,4,5.5])
        g=SArray(t)
        self.assertEqual(len(g), len(t))
        self.assertEqual(g.dtype(), float)
        glist = list(g)
        for i in range(len(glist)):
          self.assertAlmostEqual(glist[i], t[i])

        t = array.array('i',[1,2,3,4,5])
        g=SArray(t)
        self.assertEqual(len(g), len(t))
        self.assertEqual(g.dtype(), int)
        glist = list(g)
        for i in range(len(glist)):
          self.assertEqual(glist[i], t[i])


    def test_save_load(self):
        # Make sure these files don't exist before testing
        self._remove_sarray_files("intarr")
        self._remove_sarray_files("fltarr")
        self._remove_sarray_files("strarr")
        self._remove_sarray_files("vecarr")
        self._remove_sarray_files("listarr")
        self._remove_sarray_files("dictarr")

        sint = SArray(self.int_data, int)
        sflt = SArray([float(x) for x in self.int_data], float)
        sstr = SArray([str(x) for x in self.int_data], str)
        svec = SArray(self.vec_data, array.array)
        slist = SArray(self.list_data, list)
        sdict = SArray(self.dict_data, dict)

        sint.save('intarr.sidx')
        sflt.save('fltarr.sidx')
        sstr.save('strarr.sidx')
        svec.save('vecarr.sidx')
        slist.save('listarr.sidx')
        sdict.save('dictarr.sidx')

        sint2 = SArray('intarr.sidx')
        sflt2 = SArray('fltarr.sidx')
        sstr2 = SArray('strarr.sidx')
        svec2 = SArray('vecarr.sidx')
        slist2 = SArray('listarr.sidx')
        sdict2 = SArray('dictarr.sidx')
        self.assertRaises(IOError, lambda: SArray('__no_such_file__.sidx'))

        self.__test_equal(sint2, self.int_data, int)
        self.__test_equal(sflt2, [float(x) for x in self.int_data], float)
        self.__test_equal(sstr2, [str(x) for x in self.int_data], str)
        self.__test_equal(svec2, self.vec_data, array.array)
        self.__test_equal(slist2, self.list_data, list)
        self.__test_equal(sdict2, self.dict_data, dict)

        # Bad permission
        test_dir = 'test_dir'
        if os.path.exists(test_dir):
            os.removedirs(test_dir)
        os.makedirs(test_dir, mode=0000)
        with self.assertRaises(IOError):
            sint.save(os.path.join(test_dir, 'bad.sidx'))

        # Permissions will affect this test first, so no need
        # to write something here
        with self.assertRaises(IOError):
          sint3 = SArray(os.path.join(test_dir, 'bad.sidx'))

        os.removedirs(test_dir)

        #cleanup
        del sint2
        del sflt2
        del sstr2
        del svec2
        del slist2
        del sdict2
        self._remove_sarray_files("intarr")
        self._remove_sarray_files("fltarr")
        self._remove_sarray_files("strarr")
        self._remove_sarray_files("vecarr")
        self._remove_sarray_files("listarr")
        self._remove_sarray_files("dictarr")

    def test_save_load_text(self):
        self._remove_single_file('txt_int_arr.txt')
        sint = SArray(self.int_data, int)
        sint.save('txt_int_arr.txt')
        self.assertTrue(os.path.exists('txt_int_arr.txt'))
        f = open('txt_int_arr.txt')
        lines = f.readlines()
        for i in range(len(sint)):
            self.assertEquals(int(lines[i]), sint[i])
        self._remove_single_file('txt_int_arr.txt')

        self._remove_single_file('txt_int_arr')
        sint.save('txt_int_arr', format='text')
        self.assertTrue(os.path.exists('txt_int_arr'))
        f = open('txt_int_arr')
        lines = f.readlines()
        for i in range(len(sint)):
            self.assertEquals(int(lines[i]), sint[i])
        self._remove_single_file('txt_int_arr')

    def _remove_single_file(self, filename):
        try:
            os.remove(filename)
        except:
            pass

    def _remove_sarray_files(self, prefix):
        filelist = [ f for f in os.listdir(".") if f.startswith(prefix) ]
        for f in filelist:
            shutil.rmtree(f)

    def test_transform(self):
        sa_char = SArray(self.url, str)
        sa_int = sa_char.apply(lambda char: ord(char), int)

        expected_output = [x for x in range(ord('a'), ord('a') + 26)]
        self.__test_equal(sa_int, expected_output, int)

        # Test randomness across segments, randomized sarray should have different elemetns.
        sa_random = SArray(range(0, 16), int).apply(lambda x: random.randint(0, 1000), int)
        vec = list(sa_random.head(sa_random.size()))
        self.assertFalse(all([x == vec[0] for x in vec]))

        # test transform with missing values
        sa = SArray([1,2,3,None,4,5])
        sa1 = sa.apply(lambda x : x + 1)
        self.__test_equal(sa1, [2,3,4,None,5,6], int)

    def test_transform_with_multiple_lambda(self):
        sa_char = SArray(self.url, str)
        sa_int = sa_char.apply(lambda char: ord(char), int)
        sa2_int = sa_int.apply(lambda val: val + 1, int)

        expected_output = [x for x in range(ord('a') + 1, ord('a') + 26 + 1)]
        self.__test_equal(sa2_int, expected_output, int)

    def test_transform_with_exception(self):
        sa_char = SArray(['a' for i in xrange(10000)], str)
        # # type mismatch exception
        self.assertRaises(TypeError, lambda: sa_char.apply(lambda char: char, int).head(1))
        # # divide by 0 exception
        self.assertRaises(ZeroDivisionError, lambda: sa_char.apply(lambda char: ord(char) / 0, float))

    def test_transform_with_type_inference(self):
        sa_char = SArray(self.url, str)
        sa_int = sa_char.apply(lambda char: ord(char))

        expected_output = [x for x in range(ord('a'), ord('a') + 26)]
        self.__test_equal(sa_int, expected_output, int)

        sa_bool = sa_char.apply(lambda char: ord(char) > ord('c'))
        expected_output = [int(x > ord('c')) for x in range(ord('a'), ord('a') + 26)]
        self.__test_equal(sa_bool, expected_output, int)

        # # divide by 0 exception
        self.assertRaises(ZeroDivisionError, lambda: sa_char.apply(lambda char: ord(char) / 0))

        # Test randomness across segments, randomized sarray should have different elemetns.
        sa_random = SArray(range(0, 16), int).apply(lambda x: random.randint(0, 1000))
        vec = list(sa_random.head(sa_random.size()))
        self.assertFalse(all([x == vec[0] for x in vec]))

    def test_transform_on_lists(self):
        sa_int =  SArray(self.int_data, int)
        sa_vec2 = sa_int.apply(lambda x: [x, x+1, str(x)])
        expected = [[i, i + 1, str(i)] for i in self.int_data]
        self.__test_equal(sa_vec2, expected, list)
        sa_int_again = sa_vec2.apply(lambda x: int(x[0]))
        self.__test_equal(sa_int_again, self.int_data, int)

        # transform from vector to vector
        sa_vec = SArray(self.vec_data, array.array)
        sa_vec2 = sa_vec.apply(lambda x: x)
        self.__test_equal(sa_vec2, self.vec_data, array.array)

        # transform on list
        sa_list = SArray(self.list_data, list)
        sa_list2 = sa_list.apply(lambda x: x)
        self.__test_equal(sa_list2, self.list_data, list)

        # transform dict to list
        sa_dict = SArray(self.dict_data, dict)
        sa_list = sa_dict.apply(lambda x: x.keys())
        self.__test_equal(sa_list, [x.keys() for x in self.dict_data], list)

    def test_transform_dict(self):
        # lambda accesses dict
        sa_dict = SArray([{'a':1}, {1:2}, {'c': 'a'}, None], dict)
        sa_bool_r = sa_dict.apply(lambda x: x.has_key('a') if x != None else None, skip_undefined=False)
        expected_output = [1, 0, 0, None]
        self.__test_equal(sa_bool_r, expected_output, int)

        # lambda returns dict
        expected_output = [{'a':1}, {1:2}, None, {'c': 'a'}]
        sa_dict = SArray(expected_output, dict)
        lambda_out = sa_dict.apply(lambda x: x)
        self.__test_equal(lambda_out, expected_output, dict)

    def test_filter_dict(self):
        data = [{'a':1}, {1:2}, None, {'c': 'a'}]
        expected_output = [{'a':1}]
        sa_dict = SArray(expected_output, dict)
        ret = sa_dict.filter(lambda x: x.has_key('a'))
        self.__test_equal(ret, expected_output, dict)

        # try second time to make sure the lambda system still works
        expected_output = [{1:2}]
        sa_dict = SArray(expected_output, dict)
        lambda_out = sa_dict.filter(lambda x: x.has_key(1))
        self.__test_equal(lambda_out, expected_output, dict)

    def test_filter(self):
        # test empty
        s = SArray([], float)
        no_change = s.filter(lambda x : x == 0)
        self.assertEqual(no_change.size(), 0)

        # test normal case
        s = SArray(self.int_data, int)
        middle_of_array = s.filter(lambda x: x > 3 and x < 8)
        self.assertEqual(list(middle_of_array.head(10)), [x for x in range(4,8)])

        # test normal string case
        s = SArray(self.string_data, str)
        exp_val_list = [x for x in self.string_data if x != 'world']
        # Remove all words whose second letter is not in the first half of the alphabet
        second_letter = s.filter(lambda x: len(x) > 1 and (ord(x[1]) > ord('a')) and (ord(x[1]) < ord('n')))
        self.assertEqual(list(second_letter.head(10)), exp_val_list)

        # test not-a-lambda
        def a_filter_func(x):
            return ((x > 4.4) and (x < 6.8))

        s = SArray(self.int_data, float)
        another = s.filter(a_filter_func)
        self.assertEqual(list(another.head(10)), [5.,6.])

        sa = SArray(self.float_data)

        # filter by self
        sa2 = sa[sa]
        self.assertEquals(list(sa.head(10)), list(sa2.head(10)))

        # filter by zeros
        sa_filter = SArray([0,0,0,0,0,0,0,0,0,0])
        sa2 = sa[sa_filter]
        self.assertEquals(len(sa2), 0)

        # filter by wrong size
        sa_filter = SArray([0,2,5])
        with self.assertRaises(IndexError):
            sa2 = sa[sa_filter]


    def test_any_all(self):
        s = SArray([0,1,2,3,4,5,6,7,8,9], int)
        self.assertEqual(s.any(), True)
        self.assertEqual(s.all(), False)
        s = SArray([0,0,0,0,0], int)
        self.assertEqual(s.any(), False)
        self.assertEqual(s.all(), False)

        s = SArray(self.string_data, str)
        self.assertEqual(s.any(), True)
        self.assertEqual(s.all(), True)

        s = SArray(self.int_data, int)
        self.assertEqual(s.any(), True)
        self.assertEqual(s.all(), True)

        # test empty
        s = SArray([], int)
        self.assertEqual(s.any(), False)
        self.assertEqual(s.all(), True)

        s = SArray([[], []], array.array)
        self.assertEqual(s.any(), False)
        self.assertEqual(s.all(), False)

        s = SArray([[],[1.0]], array.array)
        self.assertEqual(s.any(), True)
        self.assertEqual(s.all(), False)

    def test_astype(self):
        # test empty
        s = SArray([], int)
        as_out = s.astype(float)
        self.assertEqual(as_out.dtype(), float)

        # test float -> int
        s = SArray(map(lambda x: x+0.2, self.float_data), float)
        as_out = s.astype(int)
        self.assertEqual(list(as_out.head(10)), self.int_data)

        # test int->string
        s = SArray(self.int_data, int)
        as_out = s.astype(str)
        self.assertEqual(list(as_out.head(10)), map(lambda x: str(x), self.int_data))

        i_out = as_out.astype(int)
        self.assertEqual(list(i_out.head(10)), list(s.head(10)))

        s = SArray(self.vec_data, array.array)

        with self.assertRaises(RuntimeError):
            s.astype(int)
        with self.assertRaises(RuntimeError):
            s.astype(float)

        s = SArray(["a","1","2","3"])
        with self.assertRaises(RuntimeError):
            s.astype(int)

        self.assertEqual(list(s.astype(int,True).head(4)), [None,1,2,3])

        s = SArray(["[1 2 3]","[4;5]"])
        ret = list(s.astype(array.array).head(2))
        self.assertEqual(ret, [array.array('d',[1,2,3]),array.array('d',[4,5])])

        s = SArray(["[1,\"b\",3]","[4,5]"])
        ret = list(s.astype(list).head(2))
        self.assertEqual(ret, [[1,"b",3],[4,5]])

        s = SArray(["{\"a\":2,\"b\":3}","{}"])
        ret = list(s.astype(dict).head(2))
        self.assertEqual(ret, [{"a":2,"b":3},{}])

        s = SArray(["[1abc]"])
        ret = list(s.astype(list).head(1))
        self.assertEqual(ret, [["1abc"]])

        s = SArray(["{1xyz:1a,2b:2}"])
        ret = list(s.astype(dict).head(1))
        self.assertEqual(ret, [{"1xyz":"1a","2b":2}])

    def test_clip(self):
        # invalid types
        s = SArray(self.string_data, str)
        with self.assertRaises(RuntimeError):
            s.clip(25,26)
        with self.assertRaises(RuntimeError):
            s.clip_lower(25)
        with self.assertRaises(RuntimeError):
            s.clip_upper(26)

        # int w/ int, test lower and upper functions too
        # int w/float, no change
        s = SArray(self.int_data, int)
        clip_out = s.clip(3,7).head(10)
        # test that our list isn't cast to float if nothing happened
        clip_out_nc = s.clip(0.2, 10.2).head(10)
        lclip_out = s.clip_lower(3).head(10)
        rclip_out = s.clip_upper(7).head(10)
        self.assertEqual(len(clip_out), len(self.int_data))
        self.assertEqual(len(lclip_out), len(self.int_data))
        self.assertEqual(len(rclip_out), len(self.int_data))
        for i in range(0,len(clip_out)):
            if i < 2:
                self.assertEqual(clip_out[i], 3)
                self.assertEqual(lclip_out[i], 3)
                self.assertEqual(rclip_out[i], self.int_data[i])
                self.assertEqual(clip_out_nc[i], self.int_data[i])
            elif i > 6:
                self.assertEqual(clip_out[i], 7)
                self.assertEqual(lclip_out[i], self.int_data[i])
                self.assertEqual(rclip_out[i], 7)
                self.assertEqual(clip_out_nc[i], self.int_data[i])
            else:
                self.assertEqual(clip_out[i], self.int_data[i])
                self.assertEqual(clip_out_nc[i], self.int_data[i])

        # int w/float, change
        # float w/int
        # float w/float
        clip_out = s.clip(2.8, 7.2).head(10)
        fs = SArray(self.float_data, float)
        ficlip_out = fs.clip(3, 7).head(10)
        ffclip_out = fs.clip(2.8, 7.2).head(10)
        for i in range(0,len(clip_out)):
            if i < 2:
                self.assertAlmostEqual(clip_out[i], 2.8)
                self.assertAlmostEqual(ffclip_out[i], 2.8)
                self.assertAlmostEqual(ficlip_out[i], 3.)
            elif i > 6:
                self.assertAlmostEqual(clip_out[i], 7.2)
                self.assertAlmostEqual(ffclip_out[i], 7.2)
                self.assertAlmostEqual(ficlip_out[i], 7.)
            else:
                self.assertAlmostEqual(clip_out[i], self.float_data[i])
                self.assertAlmostEqual(ffclip_out[i], self.float_data[i])
                self.assertAlmostEqual(ficlip_out[i], self.float_data[i])

        vs = SArray(self.vec_data, array.array);
        clipvs = vs.clip(3, 7).head(100)
        self.assertEqual(len(clipvs), len(self.vec_data));
        for i in range(0, len(clipvs)):
            a = clipvs[i]
            b = self.vec_data[i]
            self.assertEqual(len(a), len(b))
            for j in range(0, len(b)):
                if b[j] < 3:
                    b[j] = 3
                elif b[j] > 7:
                    b[j] = 7
            self.assertEqual(a, b)

    def test_missing(self):
        s=SArray(self.int_data, int)
        self.assertEqual(s.num_missing(), 0)
        s=SArray(self.int_data + [None], int)
        self.assertEqual(s.num_missing(), 1)

        s=SArray(self.float_data, float)
        self.assertEqual(s.num_missing(), 0)
        s=SArray(self.float_data + [None], float)
        self.assertEqual(s.num_missing(), 1)

        s=SArray(self.string_data, str)
        self.assertEqual(s.num_missing(), 0)
        s=SArray(self.string_data + [None], str)
        self.assertEqual(s.num_missing(), 1)

        s=SArray(self.vec_data, array.array)
        self.assertEqual(s.num_missing(), 0)
        s=SArray(self.vec_data + [None], array.array)
        self.assertEqual(s.num_missing(), 1)


    def test_nonzero(self):
        # test empty
        s = SArray([],int)
        nz_out = s.nnz()
        self.assertEqual(nz_out, 0)

        # test all nonzero
        s = SArray(self.float_data, float)
        nz_out = s.nnz()
        self.assertEqual(nz_out, len(self.float_data))

        # test all zero
        s = SArray([0 for x in range(0,10)], int)
        nz_out = s.nnz()
        self.assertEqual(nz_out, 0)

        # test strings
        str_list = copy.deepcopy(self.string_data)
        str_list.append("")
        s = SArray(str_list, str)
        nz_out = s.nnz()
        self.assertEqual(nz_out, len(self.string_data))

    def test_std_var(self):
        # test empty
        s = SArray([], int)
        self.assertTrue(s.std() is None)
        self.assertTrue(s.var() is None)

        # increasing ints
        s = SArray(self.int_data, int)
        self.assertAlmostEqual(s.var(), 8.25)
        self.assertAlmostEqual(s.std(), 2.8722813)

        # increasing floats
        s = SArray(self.float_data, float)
        self.assertAlmostEqual(s.var(), 8.25)
        self.assertAlmostEqual(s.std(), 2.8722813)

        # vary ddof
        self.assertAlmostEqual(s.var(ddof=3), 11.7857143)
        self.assertAlmostEqual(s.var(ddof=6), 20.625)
        self.assertAlmostEqual(s.var(ddof=9), 82.5)
        self.assertAlmostEqual(s.std(ddof=3), 3.4330328)
        self.assertAlmostEqual(s.std(ddof=6), 4.5414755)
        self.assertAlmostEqual(s.std(ddof=9), 9.08295106)

        # bad ddof
        with self.assertRaises(RuntimeError):
            s.var(ddof=11)
        with self.assertRaises(RuntimeError):
            s.std(ddof=11)
        # bad type
        s = SArray(self.string_data, str)
        with self.assertRaises(RuntimeError):
            s.std()
        with self.assertRaises(RuntimeError):
            s.var()

        # overflow test
        huge_int = 9223372036854775807
        s = SArray([1, huge_int], int)
        self.assertAlmostEqual(s.var(), 21267647932558653957237540927630737409.0)
        self.assertAlmostEqual(s.std(), 4611686018427387900.0)

    def test_tail(self):
        # test empty
        s = SArray([], int)
        self.assertEqual(len(s.tail()), 0)

        # test standard tail
        s = SArray([x for x in range(0,40)], int)
        self.assertEqual(s.tail(), [x for x in range(30,40)])

        # smaller amount
        self.assertEqual(s.tail(3), [x for x in range(37,40)])

        # larger amount
        self.assertEqual(s.tail(40), [x for x in range(0,40)])

        # too large
        self.assertEqual(s.tail(81), [x for x in range(0,40)])

    def test_max_min_sum_mean(self):
        # negative and positive
        s = SArray([-2,-1,0,1,2], int)
        self.assertEqual(s.max(), 2)
        self.assertEqual(s.min(), -2)
        self.assertEqual(s.sum(), 0)
        self.assertAlmostEqual(s.mean(), 0.)

        # test valid and invalid types
        s = SArray(self.string_data, str)
        with self.assertRaises(RuntimeError):
            s.max()
        with self.assertRaises(RuntimeError):
            s.min()
        with self.assertRaises(RuntimeError):
            s.sum()
        with self.assertRaises(RuntimeError):
            s.mean()

        s = SArray(self.int_data, int)
        self.assertEqual(s.max(), 10)
        self.assertEqual(s.min(), 1)
        self.assertEqual(s.sum(), 55)
        self.assertAlmostEqual(s.mean(), 5.5)

        s = SArray(self.float_data, float)
        self.assertEqual(s.max(), 10.)
        self.assertEqual(s.min(), 1.)
        self.assertEqual(s.sum(), 55.)
        self.assertAlmostEqual(s.mean(), 5.5)

        # test all negative
        s = SArray(map(lambda x: x*-1, self.int_data), int)
        self.assertEqual(s.max(), -1)
        self.assertEqual(s.min(), -10)
        self.assertEqual(s.sum(), -55)
        self.assertAlmostEqual(s.mean(), -5.5)

        # test empty
        s = SArray([], float)
        self.assertTrue(s.max() is None)
        self.assertTrue(s.min() is None)
        self.assertTrue(s.sum() is None)
        self.assertTrue(s.mean() is None)

        # test big ints
        huge_int = 9223372036854775807
        s = SArray([1, huge_int], int)
        self.assertEqual(s.max(), huge_int)
        self.assertEqual(s.min(), 1)
        # yes, we overflow
        self.assertEqual(s.sum(), (huge_int+1)*-1)
        # ...but not here
        self.assertAlmostEqual(s.mean(), 4611686018427387904.)

        a = SArray([[1,2],[1,2],[1,2]], array.array)
        self.assertEqual(a.sum(), array.array('d', [3,6]))
        self.assertEqual(a.mean(), array.array('d', [1,2]))
        with self.assertRaises(RuntimeError):
            a.max()
        with self.assertRaises(RuntimeError):
            a.min()

        a = SArray([[1,2],[1,2],[1,2,3]], array.array)
        with self.assertRaises(RuntimeError):
            a.sum()
        with self.assertRaises(RuntimeError):
            a.mean()

    def test_python_special_functions(self):
        s = SArray([], int)
        self.assertEqual(len(s), 0)
        self.assertEqual(str(s), '[]')
        self.assertEqual(bool(s), False)

        # increasing ints
        s = SArray(self.int_data, int)
        self.assertEqual(len(s), len(self.int_data))
        self.assertEqual(str(s), str(self.int_data))
        self.assertEqual(bool(s), True)

        realsum = sum(self.int_data)
        sum1 = sum([x for x in s])
        sum2 = s.sum()
        sum3 = s.apply(lambda x:x, int).sum()

        self.assertEquals(sum1, realsum)
        self.assertEquals(sum2, realsum)
        self.assertEquals(sum3, realsum)


    def test_scalar_operators(self):
        s=np.array([1,2,3,4,5,6,7,8,9,10]);
        t = SArray(s, int)
        self.__test_equal(t + 1, list(s + 1), int)
        self.__test_equal(t - 1, list(s - 1), int)
        # we handle division differently. All divisions cast to float
        self.__test_equal(t / 2, list(s / 2.0), float)
        self.__test_equal(t * 2, list(s * 2), int)
        self.__test_equal(t < 5, list(s < 5), int)
        self.__test_equal(t > 5, list(s > 5), int)
        self.__test_equal(t <= 5, list(s <= 5), int)
        self.__test_equal(t >= 5, list(s >= 5), int)
        self.__test_equal(t == 5, list(s == 5), int)
        self.__test_equal(t != 5, list(s != 5), int)
        self.__test_equal(1.5 + t, list(1.5 + s), float)
        self.__test_equal(1.5 - t, list(1.5 - s), float)
        self.__test_equal(2.0 / t, list(2.0 / s), float)
        self.__test_equal(2 / t, list(2.0 / s), float)
        self.__test_equal(2.5 * t, list(2.5 * s), float)


        s=["a","b","c"]
        t = SArray(s, str)
        self.__test_equal(t + "x", [i + "x" for i in s], str)
        with self.assertRaises(RuntimeError):
            t - 'x'
        with self.assertRaises(RuntimeError):
            t * 'x'
        with self.assertRaises(RuntimeError):
            t / 'x'

        s = SArray(self.vec_data, array.array)
        self.__test_equal(s + 1, [array.array('d', [float(j) + 1 for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s - 1, [array.array('d', [float(j) - 1 for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s * 2, [array.array('d', [float(j) * 2 for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s / 2, [array.array('d', [float(j) / 2 for j in i]) for i in self.vec_data], array.array)
        s = SArray([1,2,3,4,None])
        self.__test_equal(s == None, [0,0,0,0,1], int)
        self.__test_equal(s != None, [1,1,1,1,0], int)

    def test_vector_operators(self):
        s=np.array([1,2,3,4,5,6,7,8,9,10]);
        s2=np.array([5,4,3,2,1,10,9,8,7,6]);
        t = SArray(s, int)
        t2 = SArray(s2, int)
        self.__test_equal(t + t2, list(s + s2), int)
        self.__test_equal(t - t2, list(s - s2), int)
        # we handle division differently. All divisions cast to float
        self.__test_equal(t / t2, list(s.astype(float) / s2), float)
        self.__test_equal(t * t2, list(s * s2), int)
        self.__test_equal(t < t2, list(s < s2), int)
        self.__test_equal(t > t2, list(s > s2), int)
        self.__test_equal(t <= t2, list(s <= s2), int)
        self.__test_equal(t >= t2, list(s >= s2), int)
        self.__test_equal(t == t2, list(s == s2), int)
        self.__test_equal(t != t2, list(s != s2), int)

        s = SArray(self.vec_data, array.array)
        self.__test_equal(s + s, [array.array('d', [float(j) + float(j) for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s - s, [array.array('d', [float(j) - float(j) for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s * s, [array.array('d', [float(j) * float(j) for j in i]) for i in self.vec_data], array.array)
        self.__test_equal(s / s, [array.array('d', [float(j) / float(j) for j in i]) for i in self.vec_data], array.array)
        t = SArray(self.float_data, float)

        self.__test_equal(s + t, [array.array('d', [float(j) + i[1] for j in i[0]]) for i in zip(self.vec_data, self.float_data)], array.array)
        self.__test_equal(s - t, [array.array('d', [float(j) - i[1] for j in i[0]]) for i in zip(self.vec_data, self.float_data)], array.array)
        self.__test_equal(s * t, [array.array('d', [float(j) * i[1] for j in i[0]]) for i in zip(self.vec_data, self.float_data)], array.array)
        self.__test_equal(s / t, [array.array('d', [float(j) / i[1] for j in i[0]]) for i in zip(self.vec_data, self.float_data)], array.array)

        s = SArray([1,2,3,4,None])
        self.assertTrue((s==s).all())
        s = SArray([1,2,3,4,None])
        self.assertFalse((s!=s).any())

    def test_logical_ops(self):
        s=np.array([0,0,0,0,1,1,1,1]);
        s2=np.array([0,1,0,1,0,1,0,1]);
        t = SArray(s, int)
        t2 = SArray(s2, int)
        self.__test_equal(t & t2, list(((s & s2) > 0).astype(int)), int)
        self.__test_equal(t | t2, list(((s | s2) > 0).astype(int)), int)

    def test_string_operators(self):
        s=["a","b","c","d","e","f","g","h","i","j"];
        s2=["e","d","c","b","a","j","i","h","g","f"];

        t = SArray(s, str)
        t2 = SArray(s2, str)
        self.__test_equal(t + t2, ["".join(x) for x in zip(s,s2)], str)
        self.__test_equal(t + "x", [x + "x" for x in s], str)
        self.__test_equal(t < t2, [x < y for (x,y) in zip(s,s2)], int)
        self.__test_equal(t > t2, [x > y for (x,y) in zip(s,s2)], int)
        self.__test_equal(t == t2, [x == y for (x,y) in zip(s,s2)], int)
        self.__test_equal(t != t2, [x != y for (x,y) in zip(s,s2)], int)
        self.__test_equal(t <= t2, [x <= y for (x,y) in zip(s,s2)], int)
        self.__test_equal(t >= t2, [x >= y for (x,y) in zip(s,s2)], int)


    def test_vector_operator_missing_propagation(self):
        t = SArray([1,2,3,4,None,6,7,8,9,None], float) # missing 4th and 9th
        t2 = SArray([None,4,3,2,np.nan,10,9,8,7,6], float) # missing 0th and 4th
        self.assertEquals(len((t + t2).dropna()), 7);
        self.assertEquals(len((t - t2).dropna()), 7);
        self.assertEquals(len((t * t2).dropna()), 7);

    def test_dropna(self):
        no_nas = ['strings', 'yeah', 'nan', 'NaN', 'NA', 'None']
        t = SArray(no_nas)
        self.assertEquals(len(t.dropna()), 6)
        self.assertEquals(list(t.dropna()), no_nas)
        t2 = SArray([None,np.nan])
        self.assertEquals(len(t2.dropna()), 0)
        self.assertEquals(list(SArray(self.int_data).dropna()), self.int_data)
        self.assertEquals(list(SArray(self.float_data).dropna()), self.float_data)

    def test_fillna(self):
        # fillna shouldn't fill anything
        no_nas = ['strings', 'yeah', 'nan', 'NaN', 'NA', 'None']
        t = SArray(no_nas)
        out = t.fillna('hello')
        self.assertEquals(list(out), no_nas)

        # Normal integer case (float auto casted to int)
        t = SArray([53,23,None,np.nan,5])
        self.assertEquals(list(t.fillna(-1.0)), [53,23,-1,-1,5])

        # dict type
        t = SArray(self.dict_data+[None])
        self.assertEquals(list(t.fillna({1:'1'})), self.dict_data+[{1:'1'}])

        # list type
        t = SArray(self.list_data+[None])
        self.assertEquals(list(t.fillna([0,0,0])), self.list_data+[[0,0,0]])

        # vec type
        t = SArray(self.vec_data+[None])
        self.assertEquals(list(t.fillna(array.array('f',[0.0,0.0]))), self.vec_data+[array.array('f',[0.0,0.0])])

        # empty sarray
        t = SArray()
        self.assertEquals(len(t.fillna(0)), 0)

    def test_sample(self):
        sa = SArray(data=self.int_data)
        sa_sample = sa.sample(.5, 9)
        sa_sample2 = sa.sample(.5, 9)

        self.assertEqual(sa_sample.head(), sa_sample2.head())

        for i in sa_sample:
            self.assertTrue(i in self.int_data)

        with self.assertRaises(ValueError):
            sa.sample(3)

        sa_sample = SArray().sample(.5, 9)
        self.assertEqual(len(sa_sample), 0)

    def test_vector_slice(self):
        d=[[1],[1,2],[1,2,3]]
        g=SArray(d, array.array)
        self.assertEqual(list(g.vector_slice(0).head()), [1,1,1])
        self.assertEqual(list(g.vector_slice(0,2).head()), [None,array.array('d', [1,2]),array.array('d', [1,2])])
        self.assertEqual(list(g.vector_slice(0,3).head()), [None,None,array.array('d', [1,2,3])])

        g=SArray(self.vec_data, array.array);
        self.__test_equal(g.vector_slice(0), self.float_data, float)
        self.__test_equal(g.vector_slice(0, 2), self.vec_data, array.array)

    def test_lazy_eval(self):
        sa = SArray(range(-10, 10))
        sa = sa + 1
        sa1 = sa >= 0
        sa2 = sa <= 0
        sa3 = sa[sa1 & sa2]
        item_count = sa3.size()
        self.assertEqual(item_count, 1)

    def __test_append(self, data1, data2, dtype):
        sa1 = SArray(data1, dtype)
        sa2 = SArray(data2, dtype)
        sa3 = sa1.append(sa2)
        self.__test_equal(sa3, data1 + data2, dtype)

        sa3 = sa2.append(sa1)
        self.__test_equal(sa3, data2 + data1, dtype)

    def test_append(self):
        n = len(self.int_data)
        m = n / 2

        self.__test_append(self.int_data[0:m], self.int_data[m:n], int)
        self.__test_append(self.bool_data[0:m], self.bool_data[m:n], int)
        self.__test_append(self.string_data[0:m], self.string_data[m:n], str)
        self.__test_append(self.float_data[0:m], self.float_data[m:n], float)
        self.__test_append(self.vec_data[0:m], self.vec_data[m:n], array.array)
        self.__test_append(self.dict_data[0:m], self.dict_data[m:n], dict)

    def test_append_exception(self):
        val1 = [i for i in range(1, 1000)]
        val2 = [str(i) for i in range(-10, 1)]
        sa1 = SArray(val1, int)
        sa2 = SArray(val2, str)
        with self.assertRaises(RuntimeError):
            sa3 = sa1.append(sa2)

    def test_word_count(self):
        sa = SArray(["This is someurl http://someurl!!", "中文 应该也 行", 'Сблъсъкът между'])
        expected = [{"this": 1, "someurl": 2, "is": 1, "http": 1}, {"中文": 1, "应该也": 1, "行": 1}, {"Сблъсъкът": 1, "между": 1}]
        expected2 = [{"This": 1, "someurl": 2, "is": 1, "http": 1}, {"中文": 1, "应该也": 1, "行": 1}, {"Сблъсъкът": 1, "между": 1}]
        sa1 = sa._count_words()
        self.assertEquals(sa1.dtype(), dict)
        self.__test_equal(sa1, expected, dict)

        sa1 = sa._count_words(to_lower=False)
        self.assertEquals(sa1.dtype(), dict)
        self.__test_equal(sa1, expected2, dict)

        #should fail if the input type is not string
        sa = SArray([1, 2, 3])
        with self.assertRaises(TypeError):
            sa._count_words()

    def test_ngram_count(self):
        sa_word = SArray(["I like big dogs. They are fun. I LIKE BIG DOGS", "I like.", "I like big"])
        sa_character = SArray(["Fun. is. fun","Fun is fun.","fu", "fun"])

        # Testing word n-gram functionality
        result = sa_word._count_ngrams(3)
        result2 = sa_word._count_ngrams(2)
        result3 = sa_word._count_ngrams(3,"word", to_lower=False)
        result4 = sa_word._count_ngrams(2,"word", to_lower=False)
        expected = [{'fun i like': 1, 'i like big': 2, 'they are fun': 1, 'big dogs they': 1, 'like big dogs': 2, 'are fun i': 1, 'dogs they are': 1}, {}, {'i like big': 1}]
        expected2 = [{'i like': 2, 'dogs they': 1, 'big dogs': 2, 'are fun': 1, 'like big': 2, 'they are': 1, 'fun i': 1}, {'i like': 1}, {'i like': 1, 'like big': 1}]
        expected3 = [{'I like big': 1, 'fun I LIKE': 1, 'I LIKE BIG': 1, 'LIKE BIG DOGS': 1, 'They are fun': 1, 'big dogs They': 1, 'like big dogs': 1, 'are fun I': 1, 'dogs They are': 1}, {}, {'I like big': 1}]
        expected4 = [{'I like': 1, 'like big': 1, 'I LIKE': 1, 'BIG DOGS': 1, 'are fun': 1, 'LIKE BIG': 1, 'big dogs': 1, 'They are': 1, 'dogs They': 1, 'fun I': 1}, {'I like': 1}, {'I like': 1, 'like big': 1}]



        self.assertEquals(result.dtype(), dict)
        self.__test_equal(result, expected, dict)
        self.assertEquals(result2.dtype(), dict)
        self.__test_equal(result2, expected2, dict)
        self.assertEquals(result3.dtype(), dict)
        self.__test_equal(result3, expected3, dict)
        self.assertEquals(result4.dtype(), dict)
        self.__test_equal(result4, expected4, dict)


        #Testing character n-gram functionality
        result5 = sa_character._count_ngrams(3, "character")
        result6 = sa_character._count_ngrams(2, "character")
        result7 = sa_character._count_ngrams(3, "character", to_lower=False)
        result8 = sa_character._count_ngrams(2, "character", to_lower=False)
        result9 = sa_character._count_ngrams(3, "character", to_lower=False, ignore_space=False)
        result10 = sa_character._count_ngrams(2, "character", to_lower=False, ignore_space=False)
        result11 = sa_character._count_ngrams(3, "character", to_lower=True, ignore_space=False)
        result12 = sa_character._count_ngrams(2, "character", to_lower=True, ignore_space=False)
        expected5 = [{'fun': 2, 'nis': 1, 'sfu': 1, 'isf': 1, 'uni': 1}, {'fun': 2, 'nis': 1, 'sfu': 1, 'isf': 1, 'uni': 1}, {}, {'fun': 1}]
        expected6 = [{'ni': 1, 'is': 1, 'un': 2, 'sf': 1, 'fu': 2}, {'ni': 1, 'is': 1, 'un': 2, 'sf': 1, 'fu': 2}, {'fu': 1}, {'un': 1, 'fu': 1}]
        expected7 = [{'sfu': 1, 'Fun': 1, 'uni': 1, 'fun': 1, 'nis': 1, 'isf': 1}, {'sfu': 1, 'Fun': 1, 'uni': 1, 'fun': 1, 'nis': 1, 'isf': 1}, {}, {'fun': 1}]
        expected8 = [{'ni': 1, 'Fu': 1, 'is': 1, 'un': 2, 'sf': 1, 'fu': 1}, {'ni': 1, 'Fu': 1, 'is': 1, 'un': 2, 'sf': 1, 'fu': 1}, {'fu': 1}, {'un': 1, 'fu': 1}]
        expected9 = [{' fu': 1, ' is': 1, 's f': 1, 'un ': 1, 'Fun': 1, 'n i': 1, 'fun': 1, 'is ': 1}, {' fu': 1, ' is': 1, 's f': 1, 'un ': 1, 'Fun': 1, 'n i': 1, 'fun': 1, 'is ': 1}, {}, {'fun': 1}]
        expected10 = [{' f': 1, 'fu': 1, 'n ': 1, 'is': 1, ' i': 1, 'un': 2, 's ': 1, 'Fu': 1}, {' f': 1, 'fu': 1, 'n ': 1, 'is': 1, ' i': 1, 'un': 2, 's ': 1, 'Fu': 1}, {'fu': 1}, {'un': 1, 'fu': 1}]
        expected11 = [{' fu': 1, ' is': 1, 's f': 1, 'un ': 1, 'n i': 1, 'fun': 2, 'is ': 1}, {' fu': 1, ' is': 1, 's f': 1, 'un ': 1, 'n i': 1, 'fun': 2, 'is ': 1}, {}, {'fun': 1}]
        expected12 = [{' f': 1, 'fu': 2, 'n ': 1, 'is': 1, ' i': 1, 'un': 2, 's ': 1}, {' f': 1, 'fu': 2, 'n ': 1, 'is': 1, ' i': 1, 'un': 2, 's ': 1}, {'fu': 1}, {'un': 1, 'fu': 1}]

        self.assertEquals(result5.dtype(), dict)
        self.__test_equal(result5, expected5, dict)
        self.assertEquals(result6.dtype(), dict)
        self.__test_equal(result6, expected6, dict)
        self.assertEquals(result7.dtype(), dict)
        self.__test_equal(result7, expected7, dict)
        self.assertEquals(result8.dtype(), dict)
        self.__test_equal(result8, expected8, dict)
        self.assertEquals(result9.dtype(), dict)
        self.__test_equal(result9, expected9, dict)
        self.assertEquals(result10.dtype(), dict)
        self.__test_equal(result10, expected10, dict)
        self.assertEquals(result11.dtype(), dict)
        self.__test_equal(result11, expected11, dict)
        self.assertEquals(result12.dtype(), dict)
        self.__test_equal(result12, expected12, dict)



        sa = SArray([1, 2, 3])
        with self.assertRaises(TypeError):
            #should fail if the input type is not string
            sa._count_ngrams()

        with self.assertRaises(TypeError):
            #should fail if n is not of type 'int'
            sa_word._count_ngrams(1.01)



        with self.assertRaises(ValueError):
            #should fail with invalid method
            sa_word._count_ngrams(3,"bla")

        with self.assertRaises(ValueError):
            #should fail with n <0
            sa_word._count_ngrams(0)


        with warnings.catch_warnings(True) as context:
             sa_word._count_ngrams(10)
             assert len(context) == 1



    def test_dict_keys(self):
        # self.dict_data =  [{str(i): i, i : float(i)} for i in self.int_data]
        sa = SArray(self.dict_data)
        sa_keys = sa.dict_keys()
        self.assertEquals(sa_keys, [str(i) for i in self.int_data])

        # na value
        d = [{'a': 1}, {None: 2}, {"b": None}, None]
        sa = SArray(d)
        sa_keys = sa.dict_keys()
        self.assertEquals(sa_keys, [['a'], [None], ['b'], None])

        #empty SArray
        sa = SArray()
        with self.assertRaises(RuntimeError):
            sa.dict_keys()

        # empty SArray with type
        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_keys().head(10)), [], list)

    def test_dict_values(self):
        # self.dict_data =  [{str(i): i, i : float(i)} for i in self.int_data]
        sa = SArray(self.dict_data)
        sa_values = sa.dict_values()
        self.assertEquals(sa_values, [[i, float(i)] for i in self.int_data])

        # na value
        d = [{'a': 1}, {None: 'str'}, {"b": None}, None]
        sa = SArray(d)
        sa_values = sa.dict_values()
        self.assertEquals(sa_values, [[1], ['str'], [None], None])

        #empty SArray
        sa = SArray()
        with self.assertRaises(RuntimeError):
            sa.dict_values()

        # empty SArray with type
        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_values().head(10)), [], list)

    def test_dict_trim_by_keys(self):
        # self.dict_data =  [{str(i): i, i : float(i)} for i in self.int_data]
        d = [{'a':1, 'b': [1,2]}, {None: 'str'}, {"b": None, "c": 1}, None]
        sa = SArray(d)
        sa_values = sa.dict_trim_by_keys(['a', 'b'])
        self.assertEquals(sa_values, [{}, {None: 'str'}, {"c": 1}, None])

        #empty SArray
        sa = SArray()
        with self.assertRaises(RuntimeError):
            sa.dict_trim_by_keys([])

        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_trim_by_keys([]).head(10)), [], list)

    def test_dict_trim_by_values(self):
        # self.dict_data =  [{str(i): i, i : float(i)} for i in self.int_data]
        d = [{'a':1, 'b': 20, 'c':None}, {"b": 4, None: 5}, None]
        sa = SArray(d)
        sa_values = sa.dict_trim_by_values(5,10)
        self.assertEquals(sa_values, [{'b': 20, 'c':None}, {None:5}, None])

        # no upper key
        sa_values = sa.dict_trim_by_values(2)
        self.assertEquals(sa_values, [{'b': 20}, {"b": 4, None:5}, None])

        # no param
        sa_values = sa.dict_trim_by_values()
        self.assertEquals(sa_values, [{'a':1, 'b': 20, 'c':None}, {"b": 4, None: 5}, None])

        # no lower key
        sa_values = sa.dict_trim_by_values(upper=7)
        self.assertEquals(sa_values, [{'a':1, 'c':None}, {"b": 4, None: 1}, None])

        #empty SArray
        sa = SArray()
        with self.assertRaises(RuntimeError):
            sa.dict_trim_by_values()

        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_trim_by_values().head(10)), [], list)

    def test_dict_has_any_keys(self):
        d = [{'a':1, 'b': 20, 'c':None}, {"b": 4, None: 5}, None, {'a':0}]
        sa = SArray(d)
        sa_values = sa.dict_has_any_keys([])
        self.assertEquals(sa_values, [0,0,0,0])

        sa_values = sa.dict_has_any_keys(['a'])
        self.assertEquals(sa_values, [1,0,0,1])

        # one value is auto convert to list
        sa_values = sa.dict_has_any_keys("a")
        self.assertEquals(sa_values, [1,0,0,1])

        sa_values = sa.dict_has_any_keys(['a', 'b'])
        self.assertEquals(sa_values, [1,1,0,1])

        with self.assertRaises(TypeError):
            sa.dict_has_any_keys()

        #empty SArray
        sa = SArray()
        with self.assertRaises(TypeError):
            sa.dict_has_any_keys()

        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_has_any_keys([]).head(10)), [], list)

    def test_dict_has_all_keys(self):
        d = [{'a':1, 'b': 20, 'c':None}, {"b": 4, None: 5}, None, {'a':0}]
        sa = SArray(d)
        sa_values = sa.dict_has_all_keys([])
        self.assertEquals(sa_values, [1,1,0,1])

        sa_values = sa.dict_has_all_keys(['a'])
        self.assertEquals(sa_values, [1,0,0,1])

        # one value is auto convert to list
        sa_values = sa.dict_has_all_keys("a")
        self.assertEquals(sa_values, [1,0,0,1])

        sa_values = sa.dict_has_all_keys(['a', 'b'])
        self.assertEquals(sa_values, [1,0,0,0])

        sa_values = sa.dict_has_all_keys([None, "b"])
        self.assertEquals(sa_values, [0,1,0,0])

        with self.assertRaises(TypeError):
            sa.dict_has_all_keys()

        #empty SArray
        sa = SArray()
        with self.assertRaises(TypeError):
            sa.dict_has_all_keys()

        sa = SArray([], dict)
        self.assertEquals(list(sa.dict_has_all_keys([]).head(10)), [], list)

    def test_save_load_cleanup_file(self):
        # simlarly for SArray
        with util.TempDirectory() as f:
            sa = SArray(range(1,1000000))
            sa.save(f)

            # 17 for each sarray, 1 object.bin, 1 ini
            file_count = len(os.listdir(f))
            self.assertTrue(file_count > 2)

            # sf1 now references the on disk file
            sa1 = SArray(f);

            # create another SFrame and save to the same location
            sa2 = SArray([str(i) for i in range(1,100000)])
            sa2.save(f)

            file_count = len(os.listdir(f))
            self.assertTrue(file_count > 2)

            # now sf1 should still be accessible
            self.__test_equal(sa1, list(sa), int)

            # and sf2 is correct too
            sa3 = SArray(f)
            self.__test_equal(sa3, list(sa2), str)

            # when sf1 goes out of scope, the tmp files should be gone
            sa1 = 1
            time.sleep(1)  # give time for the files being deleted
            file_count = len(os.listdir(f))
            self.assertTrue(file_count > 2)

    # list_to_compare must have all unique values for this to work
    def __generic_unique_test(self, list_to_compare):
        test = SArray(list_to_compare + list_to_compare)
        self.assertEquals(sorted(list(test.unique())), sorted(list_to_compare))

    def test_unique(self):
        # Test empty SArray
        test = SArray([])
        self.assertEquals(list(test.unique()), [])

        # Test one value
        test = SArray([1])
        self.assertEquals(list(test.unique()), [1])

        # Test many of one value
        test = SArray([1,1,1,1,1,1,1,1,1,1,1,1,1,1,1])
        self.assertEquals(list(test.unique()), [1])

        # Test all unique values
        test = SArray(self.int_data)
        self.assertEquals(sorted(list(test.unique())), self.int_data)

        # Test an interesting sequence
        interesting_ints = [4654,4352436,5453,7556,45435,4654,5453,4654,5453,1,1,1,5,5,5,8,66,7,7,77,90,-34]
        test = SArray(interesting_ints)
        u = test.unique()
        self.assertEquals(len(u), 13)

        # We do not preserve order
        self.assertEquals(sorted(list(u)), sorted(np.unique(interesting_ints)))

        # Test other types
        self.__generic_unique_test(self.string_data[0:6])

        # only works reliably because these are values that floats can perform
        # reliable equality tests
        self.__generic_unique_test(self.float_data)

        self.__generic_unique_test(self.list_data)
        self.__generic_unique_test(self.vec_data)

        with self.assertRaises(TypeError):
            SArray(self.dict_data).unique()

    def test_item_len(self):
        # empty SArray
        test = SArray([])
        with self.assertRaises(TypeError):
            self.assertEquals(test.item_length())

        # wrong type
        test = SArray([1,2,3])
        with self.assertRaises(TypeError):
            self.assertEquals(test.item_length())

        test = SArray(['1','2','3'])
        with self.assertRaises(TypeError):
            self.assertEquals(test.item_length())

        # vector type
        test = SArray([[], [1], [1,2], [1,2,3], None])
        item_length = test.item_length();
        self.assertEquals(list(item_length), list([0, 1,2,3,None]))

        # dict type
        test = SArray([{}, {'key1': 1}, {'key2':1, 'key1':2}, None])
        self.assertEquals(list(test.item_length()), list([0, 1,2,None]))

        # list type
        test = SArray([[], [1,2], ['str', 'str2'], None])
        self.assertEquals(list(test.item_length()), list([0, 2,2,None]))

    def test_random_access(self):
        t = list(range(0,100000))
        s = SArray(t)
        # simple slices
        self.__test_equal(s[1:10000], t[1:10000], int)
        self.__test_equal(s[0:10000:3], t[0:10000:3], int)
        self.__test_equal(s[1:10000:3], t[1:10000:3], int)
        self.__test_equal(s[2:10000:3], t[2:10000:3], int)
        self.__test_equal(s[3:10000:101], t[3:10000:101], int)
        # negative slices
        self.__test_equal(s[-5:], t[-5:], int)
        self.__test_equal(s[-1:], t[-1:], int)
        self.__test_equal(s[-100:-10], t[-100:-10], int)
        self.__test_equal(s[-100:-10:2], t[-100:-10:2], int)
        # single element reads
        self.assertEquals(s[511], t[511])
        self.assertEquals(s[1912], t[1912])
        self.assertEquals(s[-1], t[-1])
        self.assertEquals(s[-10], t[-10])

        # A cache boundary
        self.assertEquals(s[32*1024-1], t[32*1024-1])
        self.assertEquals(s[32*1024], t[32*1024])
        
        # totally different
        self.assertEquals(s[19312], t[19312])

        # edge case odities
        self.__test_equal(s[10:100:100], t[10:100:100], int)
        self.__test_equal(s[-100:len(s):10], t[-100:len(t):10], int)
        self.__test_equal(s[-1:-2], t[-1:-2], int)
        self.__test_equal(s[-1:-1000:2], t[-1:-1000:2], int)
        with self.assertRaises(IndexError):
            s[len(s)]

        # with caching abilities; these should be fast, as 32K
        # elements are cached.
        for i in range(0, 100000, 100):
            self.assertEquals(s[i], t[i])
        for i in range(0, 100000, 100):
            self.assertEquals(s[-i], t[-i])

    def test_sort(self):
        test = SArray([1,2,3,5,1,4])
        ascending = SArray([1,1,2,3,4,5])
        descending = SArray([5,4,3,2,1,1])
        result = test.sort()
        self.assertEqual(result, ascending)
        result = test.sort(ascending = False)
        self.assertEqual(result, descending)

        with self.assertRaises(TypeError):
            SArray([[1,2], [2,3]]).sort()

    def test_unicode_encode_should_not_fail(self):
        g=SArray([{'a':u'\u2019'}])
        g=SArray([u'123',u'\u2019'])
        g=SArray(['123',u'\u2019'])

    def test_read_from_avro(self):
        data = """Obj\x01\x04\x16avro.schema\xec\x05{"fields": [{"type": "string", "name": "business_id"}, {"type": "string", "name": "date"}, {"type": "string", "name": "review_id"}, {"type": "int", "name": "stars"}, {"type": "string", "name": "text"}, {"type": "string", "name": "type"}, {"type": "string", "name": "user_id"}, {"type": {"type": "map", "values": "int"}, "name": "votes"}], "type": "record", "name": "review"}\x14avro.codec\x08null\x00\x0e7\x91\x0b#.\x8f\xa2H%<G\x9c\x89\x93\xfb\x04\xe8 ,sgBl3UDEcNYKwuUb92CYdA\x142009-01-25,Zj-R0ZZqIKFx56LY2su1iQ\x08\x80\x19The owner of China King had never heard of Yelp...until Jim W rolled up on China King!\n\nThe owner of China King, Michael, is very friendly and chatty.  Be Prepared to chat for a few minutes if you strike up a conversation.\n\nThe service here was terrific.  We had several people fussing over us but the primary server, Maggie was a gem.  \n\nMy wife and the kids opted for the Americanized menu and went with specials like sweet and sour chicken, shrimp in white sauce and garlic beef.  Each came came with soup, egg roll and rice.  I sampled the garlic beef which they prepared with a kung pao brown sauce (a decision Maggie and my wife arrived at after several minutes of discussion) it had a nice robust flavor and the veggies were fresh and flavorful.  I  also sampled the shrimp which were succulent and the white sauce had a little more distinctiveness to it than the same sauce at many Chinese restaurants.\n\nI ordered from the traditional menu but went not too adventurous with sizzling plate with scallops and shrimp in black pepper sauce.  Very enjoyable.  Again, succulent shrimp.  The scallops were tasty as well.  Realizing that I moved here from Boston and I go into any seafood experience with diminished expectations now that I live in the west, I have to say the scallops are among the fresher and judiciously prepared that I have had in Phoenix.\n\nOverall China King delivered a very tasty and very fresh meal.  They have a fairly extensive traditional menu which I look forward to exploring further.\n\nThanks to Christine O for her review...after reading that I knew China King was A-OK.\x0creview,P2kVk4cIWyK4e4h14RhK-Q\x06\nfunny\x08\x0cuseful\x12\x08cool\x0e\x00,arKckMf7lGNYjXjKo6DXcA\x142012-05-05,EyVfhRDlyip2ErKMOHEA-A\x08\xa4\x04We\'ve been here a few times and we love all the fresh ingredients. The pizza is good when you eat it fresh but if you like to eat your pizza cold then you\'ll be biting into hard dough. Their Nutella pizza is good. Take a menu and check out their menu and hours for specials.\x0creview,x1Yl1dpNcWCCEdpME9dg0g\x06\nfunny\x02\x0cuseful\x02\x08cool\x00\x00\x0e7\x91\x0b#.\x8f\xa2H%<G\x9c\x89\x93\xfb"""
        test_avro_file = open("test.avro", "wb")
        test_avro_file.write(data)
        test_avro_file.close()
        sa = SArray.from_avro("test.avro")
        self.assertEqual(sa.dtype(), dict)
        self.assertEqual(len(sa), 2)

    def test_from_const(self):
        g = SArray.from_const('a', 100)
        self.assertEqual(len(g), 100)
        self.assertEqual(list(g), ['a']*100)
        g = SArray.from_const(dt.datetime(2013, 5, 7, 10, 4, 10),10)
        self.assertEqual(len(g), 10)
        self.assertEqual(list(g), [dt.datetime(2013, 5, 7, 10, 4, 10,tzinfo=GMT(0))]*10)
        g = SArray.from_const(0, 0)
        self.assertEqual(len(g), 0)

        g = SArray.from_const(None, 100)
        self.assertEquals(list(g), [None] * 100)
        self.assertEqual(g.dtype(), float)

    def test_from_sequence(self):
        with self.assertRaises(TypeError):
            g = SArray.from_sequence()
        g = SArray.from_sequence(100)
        self.assertEqual(list(g), range(100))
        g = SArray.from_sequence(10, 100)
        self.assertEqual(list(g), range(10, 100))
        g = SArray.from_sequence(100, 10)
        self.assertEqual(list(g), range(100, 10))
    
    
    def test_datetime_to_str(self):
        sa = SArray(self.datetime_data)
        sa_string_back = sa.datetime_to_str()
         
        self.__test_equal(sa_string_back,['2013-05-07T10:04:10GMT+00', '1902-10-21T10:34:10GMT+00', None],str)
        
        sa = SArray([None,None,None],dtype=dt.datetime)
        sa_string_back = sa.datetime_to_str()

        self.__test_equal(sa_string_back,[None,None,None],str)
        
        sa = SArray(dtype=dt.datetime)
        sa_string_back = sa.datetime_to_str()

        self.__test_equal(sa_string_back,[],str)
        
        sa = SArray([None,None,None])
        self.assertRaises(TypeError,sa.datetime_to_str)
        
        sa = SArray()
        self.assertRaises(TypeError,sa.datetime_to_str)


    def test_str_to_datetime(self):
        sa_string = SArray(['2013-05-07T10:04:10GMT+00', '1902-10-21T10:34:10GMT+00', None])
        sa_datetime_back = sa_string.str_to_datetime()
        
        expected = [dt.datetime(2013, 5, 7, 10, 4, 10,tzinfo=GMT(0)),dt.datetime(1902, 10, 21, 10, 34, 10,tzinfo=GMT(0)),None]
        
        self.__test_equal(sa_datetime_back,expected,dt.datetime)

        sa_string = SArray([None,None,None],str)
        sa_datetime_back = sa_string.str_to_datetime()

        self.__test_equal(sa_datetime_back,[None,None,None],dt.datetime)
        
        sa_string = SArray(dtype=str)
        sa_datetime_back = sa_string.str_to_datetime()

        self.__test_equal(sa_datetime_back,[],dt.datetime)

        sa = SArray([None,None,None])
        self.assertRaises(TypeError,sa.str_to_datetime)
       

        sa = SArray()
        self.assertRaises(TypeError,sa.str_to_datetime)

        # hour without leading zero
        sa = SArray(['10/30/2014 9:01'])
        sa = sa.str_to_datetime('%m/%d/%Y %H:%M')
        expected = [dt.datetime(2014, 10, 30, 9, 1, tzinfo=GMT(0))]
        self.__test_equal(sa,expected,dt.datetime)

        # without delimiters
        sa = SArray(['10302014 0901', '10302014 2001'])
        sa = sa.str_to_datetime('%m%d%Y %H%M')
        expected = [dt.datetime(2014, 10, 30, 9, 1, tzinfo=GMT(0)),
                    dt.datetime(2014, 10, 30, 20, 1, tzinfo=GMT(0))]
        self.__test_equal(sa,expected,dt.datetime)

        # another without delimiter test
        sa = SArray(['20110623T191001'])
        sa = sa.str_to_datetime("%Y%m%dT%H%M%S%F%q")
        expected = [dt.datetime(2011, 06, 23, 19, 10, 1, tzinfo=GMT(0))]
        self.__test_equal(sa,expected,dt.datetime)

        # am pm
        sa = SArray(['10/30/2014 9:01am', '10/30/2014 9:01pm'])
        sa = sa.str_to_datetime('%m/%d/%Y %H:%M%p')
        expected = [dt.datetime(2014, 10, 30, 9, 1, tzinfo=GMT(0)),
                    dt.datetime(2014, 10, 30, 21, 1, tzinfo=GMT(0))]
        self.__test_equal(sa,expected,dt.datetime)

        sa = SArray(['10/30/2014 9:01AM', '10/30/2014 9:01PM'])
        sa = sa.str_to_datetime('%m/%d/%Y %H:%M%P')
        expected = [dt.datetime(2014, 10, 30, 9, 1, tzinfo=GMT(0)),
                    dt.datetime(2014, 10, 30, 21, 1, tzinfo=GMT(0))]
        self.__test_equal(sa,expected,dt.datetime)

        # failure 13pm
        sa = SArray(['10/30/2014 13:01pm'])
        with self.assertRaises(RuntimeError):
            sa.str_to_datetime('%m/%d/%Y %H:%M%p')

        # failure hour 13 when %l should only have up to hour 12
        sa = SArray(['10/30/2014 13:01'])
        with self.assertRaises(RuntimeError):
            sa.str_to_datetime('%m/%d/%Y %l:%M')

        with self.assertRaises(RuntimeError):
            sa.str_to_datetime('%m/%d/%Y %L:%M')
