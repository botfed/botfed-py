import inspect

# Define getargspec in terms of getfullargspec if not available
if not hasattr(inspect, 'getargspec'):
    def getargspec(func):
        import warnings
        warnings.warn("inspect.getargspec() is deprecated, use inspect.signature() or inspect.getfullargspec()", DeprecationWarning, stacklevel=2)
        return inspect.getfullargspec(func)
    
    inspect.getargspec = getargspec

import eth_account