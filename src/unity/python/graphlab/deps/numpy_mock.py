'''
Copyright (C) 2015 Dato, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''
""""
Dummy mocking module for numpy.
When numpy is not avaialbe we will import this module as graphlab.deps.numpy,
and set HAS_NUMPY to false. All methods that access numpy should check the HAS_NUMPY
flag, therefore, attributes/class/methods in this module should never be actually used.
"""
