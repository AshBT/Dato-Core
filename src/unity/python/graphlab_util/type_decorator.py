def force_cast_decorator(**type_kwargs):
    """
    Usage

    @force_cast_decorator(a=int, b=float)
    def pika(a, b):
        return a + b

    This cannot be used to check args and kwargs
    """
    def cast_to_type(val, name, valtype):
        """
        Converts a value to the given type
        """
        # special handling for SFrame to permit conversion of a 
        # list of dictionaries
        import graphlab as gl
        try:
            if hasattr(val, '__iter__') and valtype == gl.SFrame:
                try:
                    if len(val) and type(val[0]) is dict:
                        sa = gl.SArray(val)
                        return sa.unpack('')
                except:
                    pass

                try:
                    return gl.SFrame(val)
                except:
                    raise TypeError("Invalid Conversion of " + name + " to SFrame")
            else:
                return valtype(val)
        except:
            raise TypeError("Invalid Conversion of " + name + " to " + valtype.__name__)

    def cast_value(val, name, valtype):
        import graphlab as gl

        if type(valtype) is not list:
            # exact matches
            if type(val) == valtype:
                return val
            # prioritized casting
            return cast_to_type(val, name, valtype)
        else:
            # exact match
            if type(val) in valtype:
                return val
            # prioritized casting
            for i in valtype:
                try:
                    return cast_to_type(val, name, i)
                except:
                    pass
            raise TypeError("Cannot convert " + name + " to " + str(valtype))
            
    def decorator(func):
        def newf(*args, **kwargs):
            vnames = func.func_code.co_varnames
            args = list(args)
            for i in range(len(args)):
                if i < len(vnames) and vnames[i] in type_kwargs:
                    args[i] = cast_value(args[i], vnames[i], type_kwargs[vnames[i]])

            for k in kwargs:
                if k in type_kwargs:
                    kwargs[k] = cast_value(kwargs[k], k, type_kwargs[k])
            return func(*args, **kwargs)
        return newf

    return decorator
