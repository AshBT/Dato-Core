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
class builtin_info(object):
    input_type = None
    output_type = None
    predeclarations = None
    translatefn = ""

builtin_functions = {}
def registerfn(function_name, 
               input_type, 
               output_type,
               predeclarations,
               translatefn):
    t = builtin_info()
    t.input_type = input_type
    t.output_type = output_type
    t.predeclarations = predeclarations
    t.translatefn = translatefn
    global builtin_functions
    builtin_functions[function_name] = t


registerfn("math.sin", None, float, "", "math.sin")
registerfn("math.cos", None, float, "", "math.cos")
registerfn("str", None, str, "", "tostring")
registerfn("int", None, int, "", "math.floor")
registerfn("float", None, float, "", "")
registerfn("list", None, [], "", "list")
registerfn("dict", None, {}, "", "dict")
registerfn("len", None, int, "", "#")
registerfn("sum", None, int, "sum = require 'pl.seq'.sum", "sum")
registerfn("dict.keys", {}, [], "", "(function() return list(dict.keys(%s)) end)")
registerfn("dict.values", {}, [], "", "(function() return list(dict.values(%s)) end)")
registerfn("range", None, int, "ranger = require 'pl.seq'.range\n\
function range(a, b, c)\n\
if c then\n\
        return ranger(a,b,c-1)\n\
elseif b then\n\
        return ranger(a,b-1)\n\
elseif a then\n\
        return ranger(0, a-1)\n\
end\n\
end",
"range")
registerfn("str.join", [], str, "", "(function(x) return x:join(%s) end)")
registerfn("str.split", str, [str], "", "(function(delim) return list.split(%s, delim) end)")
