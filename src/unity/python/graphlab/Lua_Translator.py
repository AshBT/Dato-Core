#!/usr/bin/env python

"""
TO-DO:
* Define a constructor if one is not present in the Python File
     * Actually, we should ALWAYS define the "new" constructor
       ourselves.  Need to track the parameters to the __init__
       function so we can call it from our "new"
* Find and Track instance variables so they can be set in the
  constructor - Partially done.
* Internal function translation
* Translate this code to lua, and set up calls to the C modules.
"""

import sys
import parser
import symbol
import token
import ast
from lua_python_functions import builtin_functions

class FunctionType(object):
    input_type = None
    output_type_defined = False
    output_type = None
    def __repr__(self):
        return str(self)
    def __str__(self):
        return "(" + str(self.input_type) + ") -> (" + str(self.output_type) + ")"

def get_type_name(x):
    if type(x) == dict:
        return "dict"
    elif type(x) == list:
        return "list"
    elif x == None:
        return "None"
    else:
        return x.__name__

def get_inner_type(x):
    if type(x) == list or type(x) == dict:
        atypes = set([type(i) for i in x])
        if len(atypes) == 0:
            return None
        elif len(atypes) == 1:
            return list(atypes)[0]
    else:
        return None

class type_inference_visitor(ast.NodeVisitor):
    function_stack = []
    known_types = {}
    ast_node_to_type = {}
    # the mechanism in place is that
    # frame_i = self.known_types[i] gets the known variables in stack frame i
    #
    # frame_i[varname] is then the known type of the variable
    # If type is None, its type is unknown
    # If the type is a list (i.e. [int, int, str]) then the variable is known
    #     to be a list of elements, of the given types
    # If the type is a dictionary (i.e. ["a":int, "b":int]) then the variable is 
    #      known to be a dictionary of elements of the given types
    # If the type is FunctionType, it is a function with that input and output
   
    def __init__(self):
        self.currentScopeLevel = 0
        self.known_types[self.currentScopeLevel] = {}
        for x in builtin_functions:
            f = FunctionType()
            f.input_type = builtin_functions[x].input_type
            f.output_type = builtin_functions[x].output_type
            f.output_type_defined = True
            self.known_types[0][x] = f

    def parse(self, ast):
        return self.visit(ast)

    # gets a type of a variable name 
    def get_type(self, x):
        for i in range(self.currentScopeLevel, -1, -1):
            if x in self.known_types[i]:
                return self.known_types[i][x]
        return None
        
    def getScopeString(self):
        retVal = ""
        for x in range(self.currentScopeLevel):
            retVal = retVal + "\t"
        return retVal

    def add_type(self, x, t):
        self.known_types[self.currentScopeLevel][x] = t

    def increaseScope(self):
        self.currentScopeLevel = self.currentScopeLevel + 1
        if not self.currentScopeLevel in self.known_types:
            self.known_types[self.currentScopeLevel] = {}
        self.clearScope()

    def clearScope(self):
        self.known_types[self.currentScopeLevel] = {}

    def decreaseScope(self):
        for i in self.known_types[self.currentScopeLevel].keys():
            inner_scope_type = self.known_types[self.currentScopeLevel][i]
            if i in self.known_types[self.currentScopeLevel - 1]:
                outer_scope_type = self.known_types[self.currentScopeLevel - 1][i]
                if inner_scope_type != outer_scope_type:
                    self.known_types[self.currentScopeLevel - 1][i] = None
            else:
                self.known_types[self.currentScopeLevel - 1][i] = inner_scope_type
        self.currentScopeLevel = self.currentScopeLevel - 1
    
    def visit_Expr(self, node):
        return self.visit(node.value)

    def visit_Module(self, node):
        for line in node.body:
            self.visit(line)
        self.ast_node_to_type[node] = None
        return None

    def visit_Str(self, node):
        self.ast_node_to_type[node] = str
        return str

    def visit_Num(self, node):
        self.ast_node_to_type[node] = type(node.n)
        return type(node.n)

    def visit_List(self, node):
        retVal = []
        for elt in node.elts:
            retVal += [self.visit(elt)]
        self.ast_node_to_type[node] = retVal
        return retVal 

    def visit_Dict(self, node):
        retVal = {}
        for elt in range(len(node.keys)):
            if type(node.keys[elt]) == ast.Str:
                retVal[node.keys[elt].s] = self.visit(node.values[elt])
        self.ast_node_to_type[node] = retVal
        return retVal 

    def visit_Index(self, node):
        ret = self.visit(node.value)
        self.ast_node_to_type[node] = ret
        return ret

    def visit_Subscript(self, node):
        nvaluetype = self.visit(node.value)
        self.ast_node_to_type[node.value] = nvaluetype
        t = self.get_type(node.value.id)
        if type(node.slice.value) == ast.Num:
            val = node.slice.value
            if type(t) == dict and val.n in t:
                self.ast_node_to_type[node] = t[val.n]
                return t[val.n]
            elif type(t) == list and val.n < len(t):
                self.ast_node_to_type[node] = t[val.n]
                return t[val.n]
        elif type(node.slice.value) == ast.Str and type(t) == dict:
            val = node.slice.value
            if val.s in t:
                self.ast_node_to_type[node] = t[val.s]
                return t[val.s]
        self.ast_node_to_type[node] = None
        return None
        
    def visit_Print(self, node):
        for arg in node.values:
            self.visit(arg)

        self.ast_node_to_type[node] = None
        return None

    def visit_Attribute(self, node):
        s = self.visit(node.value)
        if (s != None):
            k = get_type_name(s) + "." + node.attr
            ret = self.get_type(k)
            self.ast_node_to_type[node] = ret
            return ret
        else: 
            return None

    def visit_Call(self, node):
        s = self.visit(node.func)

        # function calls can change the values of lists/dicts
        for arg in node.args:
            self.visit(arg)
            # if pass a list/dict by reference
            # the contents may change
            try:
                name = arg.id
                if (type(self.get_type(name)) == list):
                    self.add_type(name, [])
                elif (type(self.get_type(name)) == dict):
                    self.add_type(name, {})
            except:
                pass

        if s and type(s) == FunctionType:
            self.ast_node_to_type[node] = s.output_type
            return s.output_type
        else:
            self.ast_node_to_type[node] = None
            return None
            
    def visit_ListComp(self, node):
        self.increaseScope()
        for gen in node.generators:
            itertype = self.visit(gen.iter)
            innertype = get_inner_type(itertype)
            if "ifs" in dir(gen):
                for ifs in gen.ifs:
                    self.visit(gen.ifs)
            if "target" in dir(gen):
                self.visit(gen.target)
                self.ast_node_to_type[gen.target] = innertype
                self.add_type(gen.target.id, innertype)
        self.visit(node.elt)
        self.clearScope()
        self.decreaseScope()
        self.ast_node_to_type[node] = []
        return []

    def visit_GeneratorExp(self, node):
        return self.visit_ListComp(node)

    def visit_For(self, node):
        itertype = self.visit(node.iter)
        self.ast_node_to_type[node.iter] = itertype
        self.add_type(node.target.id, itertype)
        targettype = self.visit(node.target)
        self.ast_node_to_type[node.target] = targettype

        self.increaseScope()
        for line in node.body:
            self.visit(line) 
        self.decreaseScope()
        self.ast_node_to_type[node] = None
        return None

    def visit_While(self, node):
        self.increaseScope()
        for line in node.body:
            self.visit(line)
        self.decreaseScope()
        self.ast_node_to_type[node] = None
        return None
    
    def visit_IfExp(self, node):
        reta = self.visit(node.body)
        if "orelse" in dir(node) and node.orelse != None:
            retb = self.visit(node.orelse)
        if (reta == retb): 
            self.ast_node_to_type[node] = reta
            return reta
        else:
            self.ast_node_to_type[node] = None
            return None


    def visit_If(self, node):
        self.increaseScope()
        for line in node.body:
            self.visit(line)
        self.decreaseScope()

        if "orelse" in dir(node) and node.orelse != None: 
            self.increaseScope()
            for line in node.orelse:
                self.visit(line)
            self.decreaseScope()
        self.ast_node_to_type[node] = None
        return None
            
    def visit_BoolOp(self, node):
        self.ast_node_to_type[node] = bool
        return bool

    def visit_BinOp(self, node):
        ret = self.visit(node.left)
        self.visit(node.right)
        if type(ret) == list:
            self.ast_node_to_type[node] = []
            return []
        elif type(ret) == dict:
            self.ast_node_to_type[node] = {}
            return {}
        else:
            return ret

    def visit_Compare(self, node):
        self.visit(node.left)

        for x in node.comparators:
            self.visit(x)

        self.ast_node_to_type[node] = bool
        return bool
        
    def visit_Assign(self, node):
        value = self.visit(node.value)
        tnode = node.targets[0]
        if type(tnode) == ast.Name:
            target = tnode
            self.add_type(target.id, value)
            self.ast_node_to_type[node] = value
        elif type(tnode) == ast.Attribute and type(tnode.value) == ast.Name:
            target = tnode.value.id + "." + tnode.attr
            self.add_type(target, value)
            self.ast_node_to_type[node] = value
        value = self.visit(node.targets[0])
        return value
        
    def visit_AugAssign(self, node):
        self.visit(node.target) 
        self.visit(node.value)
        if type(self.get_type(node.target.id)) == list:
            self.add_type(node.target.id, [])
        elif type(self.get_type(node.target.id)) == dict:
            self.add_type(node.target.id, {})
        else:
            self.add_type(node.target.id, None)
        self.ast_node_to_type[node] = self.get_type(node.target.id)
        return None

    def visit_arguments(self, node):
        ret = ()
        for arg in node.args:
            ret += tuple([self.get_type(arg.id)])
        self.ast_node_to_type[node] = ret
        return ret


    def visit_Name(self, node):
        ret = self.get_type(node.id)
        print node.lineno,node.col_offset,":", node.id,"=", ret
        self.ast_node_to_type[node] = ret
        return ret


    def visit_Return(self, node):
        rettype = self.visit(node.value)
        lastfn = self.function_stack[len(self.function_stack) - 1]
        if not lastfn.output_type_defined:
            lastfn.output_type = rettype
            lastfn.output_type_defined = True
        elif lastfn.output_type != rettype:
            lastfn.output_type = None
        self.ast_node_to_type[node] = None
        return None

    def visit_Lambda(self, node):
        ret = self.visit_FunctionDef(node)
        self.ast_node_to_type[node] = ret
        return ret


    def visit_FunctionDef(self, node):
        """
        Need to go through the fields of this node to get the
        function name.  Will have to do this again with the parameters
        """

        funName = ""
        islambda = True
        if ("name" in dir(node)):
            funName = node.name
            islambda = False

        f = FunctionType()
        f.input_type = self.visit(node.args)
        self.function_stack.append(f)
        self.increaseScope()
        if islambda == False:
            for line in node.body:
                self.visit(line)
        else:
            lastfn = self.function_stack[len(self.function_stack) - 1]
            lastfn.outputtype = self.visit(node.body)
            lastfn.output_type_defined = True

        self.clearScope()
        self.decreaseScope()
        ret_type = self.function_stack.pop()
        if funName != "":
            self.add_type(funName, ret_type)
        self.ast_node_to_type[node] = ret_type
        return ret_type

    def visit_UAdd(self, node):
        """
        This operator in python quite literally does nothing.
        """
        return ""
    def visit_USub(self, node):
        """
        this is just the negation operator.
        """
        return "-"
    def visit_ClassDef(self, node):
        raise NotImplementedError("Classes are not implemented")

class translator_NodeVisitor(ast.NodeVisitor):
    ifFlag = False
    # a dictionary from old function name to new function name
    # the substitute will only happen once.
    rename_function = {}  
    # A dictionary from the function name to the known type of a
    # function(FunctionType).  This happens after the function name.
    function_known_types = {} 
    known_types = None
    predeclarations = []
    builtin_declarations = []

    def __init__(self, file):
        self.f = file
        self.currentScopeLevel = 0
        self.known_types = type_inference_visitor()
        self.builtin_declarations = []
        self.rename_function = {}
        self.function_known_types = {}
        self.builtin_declarations += ["list = require 'pl.List'"]
        self.builtin_declarations += ["dict = require 'pl.Map'"]

    def translate_ast(self, ast_node):
        print(ast.dump(ast_node))
        self.known_types.visit(ast_node)
        t = self.visit(ast_node)
        self.predeclarations = list(set(self.predeclarations))
        if not t: 
            t = ""
        t = "\n".join(self.builtin_declarations) + "\n" + "\n".join(self.predeclarations) + "\n" + t
        if self.f:
            self.f.write(t)
        else:
            return t

    # gets a type of an ast node
    def get_type(self, x):
        if (x in self.known_types.ast_node_to_type):
            return self.known_types.ast_node_to_type[x]
        else:
            return None
        
    """Handle Scoping"""
    def increaseScope(self):
        self.currentScopeLevel = self.currentScopeLevel + 1

    def decreaseScope(self):
        self.currentScopeLevel = self.currentScopeLevel - 1
    
    def getScopeString(self):
        retVal = ""
        for x in range(self.currentScopeLevel):
            retVal = retVal + "\t"
        return retVal
    """"End Handle Scoping"""

    def visit_Expr(self, node):
        return self.visit(node.value)

    def visit_Module(self, node):
        ret = ""
        for line in node.body:
            retline = self.visit(line)
            if retline:
                ret += retline + "\n"
        return ret

    def visit_Str(self, node):
        return "\"" + node.s + "\""

    def visit_Num(self, node):
        return str(node.n)

    def visit_List(self, node):
        retVal = "list({"
        commaFlag = 0
        for elt in node.elts:
            if commaFlag:
                retVal = retVal + ", " 
            retVal = retVal + self.visit(elt)
            commaFlag = 1
        return retVal + "})"

    def visit_Dict(self, node):
        retVal = "dict({"
        commaFlag = 0
        for elt in range(len(node.keys)):
            if commaFlag:
                retVal = retVal + ", " 
            retVal = retVal + "[" + self.visit(node.keys[elt]) + "] = " + self.visit(node.values[elt])
            commaFlag = 1
        return retVal + "})"

    def visit_Index(self, node):
        if (type(node.value) == ast.Num):
            return self.visit(node.value)
        else:
            return self.visit(node.value)

    def visit_Subscript(self, node):
        idx = self.visit(node.slice)
        nodetype = self.get_type(node.value)
        if type(self.get_type(node.value)) == list:
            return node.value.id + "[(" + idx +  ") + 1]"
        elif type(self.get_type(node.value)) == dict:
            return node.value.id + "[" + idx +  "]"
        else:
            raise NotImplementedError("Ambiguous array index at " + 
                                      str(node.lineno) + ":" + str(node.col_offset))
        
    def visit_Print(self, node):
        retVal = "print("
        commaFlag = 0
        for arg in node.values:
            if commaFlag:
                retVal = retVal + ", "
            retVal = retVal + self.visit(arg)
            commaFlag = 1

        retVal = retVal + ")"
        return retVal

    def builtin_substitute(self, name):
        if name in builtin_functions:
            if builtin_functions[name].predeclarations and len(builtin_functions[name].predeclarations) > 0:
                self.predeclarations += [builtin_functions[name].predeclarations]
            return builtin_functions[name].translatefn
        else:
            return None

    def visit_Attribute(self, node):
        s = self.visit(node.value)
        # check if its a reference to a builtin function function 
        k = get_type_name(self.get_type(node.value)) + "." + node.attr
        ret = self.builtin_substitute(k)
        if ret:
            return ret % s
        else:
            return s + "." + node.attr

    def visit_Call(self, node):
        retVal = ""
        funcName = self.visit(node.func)
        funcArgs = ""
        commaFlag = 0
        for arg in node.args:
            if commaFlag:
                funcArgs = funcArgs + ", "
            funcArgs = funcArgs + self.visit(arg)
            commaFlag = 1
                
        retVal =  funcName + "(" + funcArgs + ")"

        return retVal
            
    def visit_GeneratorExp(self, node):
        return self.visit_ListComp(node)
            
    def visit_ListComp(self, node):
        if len(node.generators) != 1:
            raise NotImplementedError("List comprehension with only one generator not implemented")
        tab = self.visit(node.generators[0].iter)
        var = node.generators[0].target.id
        vartype = self.get_type(node.generators[0].iter)
        ifs = []
        if "ifs" in dir(node.generators[0]):
            ifs = node.generators[0].ifs
        # we generate a lambda function which iterates over the table
        s = "(function() \n"
        self.increaseScope()
        s += self.getScopeString() + "local __rett__ = list({})\n"
        s += self.getScopeString() + "local __t__ = " + tab + "\n"
        if (type(vartype) == list):
            s += self.getScopeString() + "for " + var + " in list.iter(__t__) do\n"
        elif (type(vartype) == dict):
            s += self.getScopeString() + "for " + var + " in pairs(__t__) do\n"
        else:
            raise NotImplementedError("We can only iterate over dictionaries and lists")
        self.increaseScope()
        s += self.getScopeString() + "local __v__ = " + self.visit(node.elt) + "\n"
        if len(ifs) > 0:
            ifvars = []
            for i in range(len(ifs)):
                ifvar = "__t" + str(i) + "__"
                s += self.getScopeString() + "local " + ifvar + " = " + self.visit(ifs[i]) + "\n"
                ifvars += [ifvar]
            s += self.getScopeString() + "if " + " and ".join(ifvars) + " then \n"
            self.increaseScope()
        s += self.getScopeString() + "__rett__[#__rett__ + 1] = __v__\n"
        if len(ifs) > 0:
            self.decreaseScope()
            s += self.getScopeString() + "end\n"
        self.decreaseScope()
        s += self.getScopeString() + "end\n"
        s += self.getScopeString() + "return __rett__\n"
        self.decreaseScope()
        s += self.getScopeString() + "end)()"
        return s

    """
    Begin Conditionals
    """
    def getBodyString(self, body):
        retVal = ""
        for line in body:
            retVal = retVal + self.getScopeString() + self.visit(line) + "\n"

        return retVal;

    def visit_For(self, node):
        retVal = ""
        self.visit(node.iter)
        loopVar = self.visit(node.target)
        iterVar = self.visit(node.iter)
        vartype = self.get_type(node.iter)

        if (type(vartype) == list):
            retVal += self.getScopeString() + "for " + loopVar + " in list.iter(" + iterVar + ") do\n"
        elif (type(vartype) == dict):
            retVal += self.getScopeString() + "for " + loopVar + " in pairs(" + iterVar + ") do\n"
        else:
            raise NotImplementedError("We can only iterate over dictionaries and lists")

        self.increaseScope()
        #Note that body is a list.
        for line in node.body:
            retVal += self.getScopeString() + self.visit(line) + "\n"

        #loopBody = self.visit(node.body)
        self.decreaseScope()
        retVal += self.getScopeString() + "end\n"
        return retVal

    def visit_While(self, node):
        condition = self.visit(node.test)
        bodyString = ""
        self.increaseScope()
        for line in node.body:
            bodyString = self.getScopeString() + self.visit(line) + "\n"
        self.decreaseScope()
        return "while " + condition + " do\n" + bodyString + self.getScopeString() + "end"
    
    def visit_IfExp(self, node):
        ret = self.visit(node.test) + " and " + self.visit(node.body)
        if "orelse" in dir(node) and node.orelse != None:
            ret += " or " + self.visit(node.orelse)
        return ret


    def visit_If(self, node):
        retVal = ""
        topLevel = False
        if self.ifFlag:
            retVal = "elseif "
        else:
            retVal = "if "
            self.ifFlag = True
            topLevel = True

        condition = self.visit(node.test)
        retVal += condition + " then\n"
        
        self.increaseScope()
        bodyString = self.getBodyString(node.body).rstrip()
        self.decreaseScope()
        
        retVal = retVal + bodyString + "\n" + self.getScopeString()

        orelse_list = []
        for elt in node.orelse:
            orelse_list.append(self.visit(elt))

        shift=""
        if orelse_list[0].split()[0] != "elseif":
            retVal = retVal + "else\n"
            self.increaseScope()
            shift = self.getScopeString()
            self.decreaseScope()

        for line in orelse_list:
            retVal = retVal + shift + line.rstrip() + "\n"

        if topLevel:
            retVal = retVal.rstrip() + "\n" + self.getScopeString() + "end"
            self.ifFlag = False
        return retVal
            
    """
    End conditionals
    """

    """
    Binary Operators Begin
    """
    def visit_Add(self, node):
        return "+"
    def visit_Sub(self, node):
        return "-"
    def visit_Mult(self, node):
        return "*"
    def visit_Div(self, node):
        return "/"
    def visit_FloorDiv(self, node):
        return "/" #In Lua, there's only one type of division. This will have to do.
    def visit_Mod(self, node):
        return "%"
    def visit_Pow(self, node):
        return "^"
    """
    Binary Operators End
    """

    """
    Unary Operators Begin
    """
    def visit_Invert(self, node):
        """
        This returns the bitwise inversion of the operand.  For example, in python,
        ~x == -(x+1)
        !!Will need a library for this!!
        """
        return ""
    def visit_Not(self, node):
        return "not"
    def visit_UAdd(self, node):
        """
        This operator in python quite literally does nothing.
        """
        return ""
    def visit_USub(self, node):
        """
        this is just the negation operator.
        """
        return "-"
    """
    Unary Operators End
    """

    """
    Comparison Operators Begin
    """
    def visit_Eq(self, node):
        return "=="
    def visit_NotEq(self, node):
        return "~="
    def visit_Lt(self, node):
        return "<"
    def visit_LtE(self, node):
        return "<="
    def visit_Gt(self, node):
        return ">"
    def visit_GtE(self, node):
        return ">="
    def visit_Is(self, node):
        print("ERROR: Operation not supported!!!")
        return "ERROR: Operation not supported!!!"
    def visit_IsNot(self, node):
        print("ERROR: Operation not supported!!!")
        return "ERROR: Operation not supported!!!"
    def visit_In(self, node):
        print("ERROR: Operation not supported!!!")
        return "ERROR: Operation not supported!!!"
    def visit_NotIn(self, node):
        print("ERROR: Operation not supported!!!")
        return "ERROR: Operation not supported!!!"

    def visit_AugAssign(self, node):
        return self.visit(node.target) + "=" + self.visit(node.target) + self.visit(node.op) + self.visit(node.value)
    """
    Comparison Operators End
    """


    """
    expr Begin
    """
    def visit_BoolOp(self, node):
        op = self.visit(node.op)
        values = self.visit(node.values)
        return op + " " + values

    def visit_BinOp(self, node):
        l = self.visit(node.left)
        op = self.visit(node.op)
        r = self.visit(node.right)
        if (op == "+"):
            ltype = self.get_type(node.left)
            rtype = self.get_type(node.right)
            if ltype == str and rtype == str:
                retVal =  l + " .. " + r
            elif ltype == str or rtype == str:
                raise TypeError("Cannot add " + str(ltype) + " and " + str(rtype) + " at " + 
                                          str(node.lineno) + ":" + str(node.col_offset))
            elif ltype in [int, float] and rtype in [int, float]:
                retVal =  l + " + " + r
            elif ltype == None or rtype == None:
                raise NotImplementedError("Ambiguous addition at " + 
                                          str(node.lineno) + ":" + str(node.col_offset))
            else:
                retVal =  l + " + " + r
        else:
            retVal =  l + " " + op + " " + r

        return retVal

    def visit_Compare(self, node):
        l = self.visit(node.left);
        op = ""
        r = ""

        for x in node.ops:
            if op == "":
                op = self.visit(x)
            else:
                print "Error: Too many compare operations"


        for x in node.comparators:
            if r == "":
                r = self.visit(x)
            else:
                print "Error: Too many comparators"

        return l + " " + op + " " + r
        
    def visit_Assign(self, node):
        target = node.targets[0].id
        value = self.visit(node.value)
        return target + " = " + value
        

    def visit_arguments(self, node):
        #push the work down another level
        commaFlag = 0
        ret = ""
        for arg in node.args:
            #Skip the self arguements, for now
            if arg.id != "self":
                if not commaFlag:
                    commaFlag = 1
                else:
                    ret += (", ")
                ret += (arg.id)
        return ret


    def visit_Name(self, node):
        ret = self.builtin_substitute(node.id)
        if ret:
            return ret
        else:
            return node.id

    def visit_Return(self, node):
        return "return " + self.visit(node.value)

    def visit_Lambda(self, node):
        return self.visit_FunctionDef(node)


    def visit_FunctionDef(self, node):
        funName = ""
        islambda = True
        if ("name" in dir(node)):
            funName = node.name
            islambda = False
        newline = "\n"
        if islambda:
            newline = ""

        outstr = "" 
        if funName in self.rename_function:
            oldfname = funName
            funName = self.rename_function[funName]
            del self.rename_function[oldfname]
        elif funName == None and "" in self.rename_function:
            funName = self.rename_function[""]
            del self.rename_function[""]

        if funName in self.function_known_types:
            # ok. we need to reannotate this vertex
            self.known_types.increaseScope()
            i = 0
            for arg in node.args.args:
                itype = self.function_known_types[funName].input_type[i]
                self.known_types.add_type(arg.id, itype)
                i += 1
            self.known_types.visit_FunctionDef(node)
            self.known_types.decreaseScope()

        if (funName == None or len(funName) == 0):
            outstr += "function ("
        else:
            outstr += ("function " + funName + "(")
        outstr += self.visit(node.args)
        outstr += ")"
        bodyString = ""
        if (islambda):
            outstr += " return ";
        self.increaseScope()
        
        if islambda == False:
            for line in node.body:
                res = self.visit(line)
                if res:
                    bodyString = bodyString + self.getScopeString() + self.visit(line) + newline
        else:
            bodyString = bodyString + self.visit(node.body) + newline
        #because of how LUA works, have to define the instance
        #variables in the new function

        outstr += (newline + bodyString);
        outstr += (" end" + newline)
        self.decreaseScope()                
        return outstr

    def visit_ClassDef(self, node):
        raise NotImplementedError("Classes are not implemented")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("Usage:\n\t./Lua_Translator.py <FILENAME>\n")
        exit(-1)
    f = open(sys.argv[1] , 'r')
    l = f.readlines()
    f.close()
    s = ""

    for x in l:
        s = s + x

    ast_node = ast.parse(s)

    f = open(sys.argv[1].rpartition(".")[0] + "_trans.lua", 'w')
    test = translator_NodeVisitor(f)
    test.translate_ast(ast_node)
    f.close()
