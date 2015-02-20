'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import multiprocessing
import time
import unittest
import logging
import array
import graphlab.connect.main as glconnect
import graphlab.cython.cy_test_utils as cy_test_utils


def fib(i):
    if i <= 2:
        return 1
    else:
        return fib(i - 1) + fib(i - 2)


class LambdaTests(unittest.TestCase):

    def test_simple_evaluation(self):
        x = 3
        self.assertEqual(glconnect.get_unity().eval_lambda(lambda y: y + x, 0), 3)
        self.assertEqual(glconnect.get_unity().eval_lambda(lambda y: y + x, 1), 4)
        self.assertEqual(glconnect.get_unity().eval_lambda(lambda x: x.upper(), 'abc'), 'ABC')
        self.assertEqual(glconnect.get_unity().eval_lambda(lambda x: x.lower(), 'ABC'), 'abc')
        self.assertEqual(glconnect.get_unity().eval_lambda(fib, 1), 1)

    def test_exception(self):
        x = 3
        self.assertRaises(RuntimeError, glconnect.get_unity().eval_lambda, lambda y: x / y, 0)
        self.assertRaises(RuntimeError, glconnect.get_unity().parallel_eval_lambda, lambda y: x / y, [0 for i in range(10)])

    def test_parallel_evaluation(self):
        xin = 33
        repeat = 8
        # execute the task bulk using one process to get a baseline
        start_time = time.time()
        glconnect.get_unity().eval_lambda(lambda x: [fib(i) for i in x], [xin for i in range(repeat)])
        single_thread_time = time.time() - start_time
        logging.info("Single thread lambda eval takes %s secs" % single_thread_time)

        # execute the task in parallel
        start_time = time.time()
        ans_list = glconnect.get_unity().parallel_eval_lambda(lambda x: fib(x), [xin for i in range(repeat)])
        multi_thread_time = time.time() - start_time
        logging.info("Multi thread lambda eval takes %s secs" % multi_thread_time)

        # test the speed up by running in parallel
        nproc = multiprocessing.cpu_count()
        if (nproc > 1 and multi_thread_time > (single_thread_time / 1.5)):
            logging.warning("Slow parallel processing: single thread takes %s secs, multithread on %s procs takes %s secs" % (single_thread_time, nproc, multi_thread_time))

        # test accuracy
        ans = fib(xin)
        for a in ans_list:
            self.assertEqual(a, ans)

    def test_crash_recovery(self):
        ls = range(1000)

        def good_fun(x):
            return x

        def bad_fun(x):
            if (x % 251 == 0):
                cy_test_utils.force_exit_fun()  # this will force the worker process to exit
            return x
        self.assertRaises(RuntimeError, lambda: glconnect.get_unity().parallel_eval_lambda(lambda x: bad_fun(x), ls))
        glconnect.get_unity().parallel_eval_lambda(lambda x: good_fun(x), ls)
