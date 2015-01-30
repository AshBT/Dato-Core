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
from datetime import tzinfo
from datetime import timedelta
class GMT(tzinfo):
    def __init__(self,ofs=None):
        if(ofs is None):
          self.offset = 0;
        else:
          self.offset = ofs
    def utcoffset(self, dt):
        return timedelta(minutes=self.offset * 60)
    def dst(self, dt):
        return timedelta(seconds=0)
    def tzname(self,dt):
        if(self.offset >= 0):
            return "GMT +"+str(self.offset)
        elif(self.offset < 0):
            return "GMT "+str(self.offset)
    def __str__(self):
        return self.tzname(self.offset)
    def  __repr__(self):
        return self.tzname(self.offset)