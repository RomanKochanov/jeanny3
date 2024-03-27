# -*- coding: utf-8 -*-

import os
import re
import sys
import csv      
import json
import copy
import shutil
import string
import hashlib
import uuid as uuidmod
import itertools as it
import functools as ft
from datetime import date, datetime
from warnings import warn,simplefilter

# increase the csv field size limit
#csv.field_size_limit(sys.maxsize) # throws error on some versions of Python
csv.field_size_limit(1000000000)

simplefilter('always', UserWarning) # make warnings display always

# Adding a JSON-serialization to the Collection type
from json import JSONEncoder
def _default(self, obj):
    return getattr(obj.__class__, "export_to_json", _default.default)(obj)
_default.default = JSONEncoder().default
JSONEncoder.default = _default

# Simple dictionaty-based DBMS for the data-mining.
# This DBMS inherits a shema-free database ideology (like MongoDB for instance).
# Each document in the collection has it's own unique ID number.
       
# Structure of a collection:
#
# Document = {'__ID__':Integer, '__DATA__':Dictionary} : Dictionary
# Collection = [Document1, Document2, ...]

# Filtering:
# >> Col = Collection(Col1)
# >> IDs = Col.IDs(filter='var['a']+var['b']>10') # get IDs with condition 
# >> Data = Col.get(IDs)
# This way of defining filters must be avoided in the release because of 
# the obvious vulnerabilities!

# VERSION 3.0

#from collections import OrderedDict # doesn't help a lot
#from addict import Dict # takes less memory than OrderedDict

# !!! write more efficient engine on the top of built-in sqlite module?
# https://pypi.python.org/pypi/sqlite-schemaless/0.1.2
# http://yserial.sourceforge.net/
# Good idea, but what to do about open data structure? We need a version control 
# layer on the top of the database engine!

__version__ = '3.0'
print('jeanny, Ver.'+__version__)

FILENAME_ID = '$FILENAME$' # parameter defining the filename
ITEM_ID = '$UUID$' # id that uniquely identifies each item (used in redistribution of items between collections)

# stub for Python 3: redefining unicode
try:
    unicode
except NameError:
    unicode = str # in Python 3 str is unicode

def uuid():
    # http://stackoverflow.com/questions/2759644/python-multiprocessing-doesnt-play-nicely-with-uuid-uuid4    
    return str(uuidmod.UUID(bytes=os.urandom(16), version=4))    
    
# https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type %s not serializable" % type(obj))

def is_identifier(token):    
    return re.match('^[a-zA-Z_][\w_]*$',token)
    
def process_exp(raw_expression): # think about more proper name
    # change this ('a**2+2') to this ('var["a"]**2+2')
    import shlex
    tokens = list(shlex.shlex(raw_expression))    
    for i,token in enumerate(tokens):
        if is_identifier(token): tokens[i] = 'var["%s"]'%token
    return ''.join(tokens)

#class TabObject:
#    """
#    Class for string representation.
#    Used for the output of tabulate function.
#    """
#    def __init__(self,st):
#        self.__string__ = st
#    def tostr(self):
#        return self.__string__
        
class Collection:
    
    #ID_REP = '$id'

    def __init__(self,**argv):
        self.initialize(**argv)
        
    def __getitem__(self,ID):
        return self.getitem(ID)
        
    def __iter__(self):
        return iter(self.getitems())
        
    def __len__(self):
        return len(self.__dicthash__)

    #def export_to_json(self): # old and useless version
    #    """
    #    Adding a serialization to the Collection type (see above).
    #    """
    #    if self.__path__ in {'',None}:
    #        warn('The collection is not actually saved')
    #    return {
    #        '__class__':'Collection',
    #        '__name__':self.__name__,
    #        '__type__':self.__type__,
    #        '__path__':self.__path__}

    def export_to_json(self): # old and useless version
        """
        Adding a serialization to the Collection type (see above).
        """
        return self.__dicthash__
                
    def clear(self): # synonym for __init__ (TODO: get rid of it)
        self.initialize() 
        # XXX: use custom method instead of __init__
        # in order not to cause conflicts with inherited classes
        
    def __repr__(self):
        
        #buf = '%s(%s): %s\n'%(self.__name__,self.__type__,self.__path__)
        #buf += json.dumps(self.keys(),indent=2)
        #print(buf)
        #return buf
        #print(self.tabulate())
        #return self.tabulate(raw=True)
        
        #ids = self.ids()
        #buf = self.tabulate(raw=True,IDs=ids[:20])
        #if len(ids)>20: buf+='\n...'
        #return buf
        
        return 'Collection (%d lines)'%len(self.ids())

    def initialize(self,path=None,type=None,name='Default',**argv):
        self.maxid = -1
        self.order = [] # order of columns (optional)
        if path:
            if not type: raise Exception('Collection type is not specified')
            if type=='csv':
                self.import_csv(path,**argv)
            elif type=='folder':                
                self.import_folder(path,**argv)
            elif type=='jsonlist':
                self.import_json_list(path,**argv)
            elif type=='xlsx':
                self.import_xlsx(path,**argv)
            elif type=='fixcol':
                self.import_fixcol(path,**argv)
            else:
                raise Exception('Unknown type: %s'%type)
            self.__type__ = type
            self.__path__ = path
            self.__name__ = name
        else:
            self.__dicthash__ = {}
            self.__name__ = ''
            self.__path__ = './'
            self.__type__ = '__init__'
            #self.__dicthash__ = OrderedDict()
            #self.__dicthash__ = Dict() # !!! ATTENTION !!! when fetching non-existing item, creates it => non-standard behaviour
            
    def export(self,path=None,type=None,**argv):
        """
        Export the collection using several available strategies.
        """
        if path is None: path = self.__path__
        if type is None: type = self.__type__
        if type=='csv':
            self.export_csv(path,**argv)
        elif type=='folder':                
            self.export_folder(path,**argv)
        elif type=='jsonlist':
            self.export_json_list(path,**argv)
        elif type=='xlsx':
            self.export_xlsx(path,**argv)
        else:
            raise Exception('Unknown type: %s'%type)        
        
    #def __deepcopy__(self):
    #    """
    #    Function override for the deep copy.
    #    https://stackoverflow.com/questions/1500718/what-is-the-right-way-to-override-the-copy-deepcopy-operations-on-an-object-in-p
    #    """
    #    pass
    
    def copy(self,name='Default',path=''): # CAN BE BUGGY!!
        """
        Copy the collection using the deep copy feature.
        https://www.geeksforgeeks.org/copy-python-deep-copy-shallow-copy/
        """
        col = copy.deepcopy(self)
        col.__name__ = name
        col.__path__ = path # don't use the parent's path by default
        return col

    def setorder(self,order):
        self.order = order
        
    def setfloatfmt(self,floatfmt):
        self.floatfmt = floatfmt
            
    def getitem(self,ID):
        #if type(ID) in [list,tuple]:
        #    if len(ID)>1:
        #        raise Exception('ID must be aither scalar or list of 1 element')
        #    ID = ID[0]
        if ID not in self.__dicthash__:
            #raise Exception('no such ID in __dicthash__: %s'%ID)
            raise KeyError('no such ID in __dicthash__: %s'%ID) # I think that this will mess up the workflow
            #return None
        return self.__dicthash__[ID]
    
    def getitems(self,IDs=-1,mode='strict'):
        """
        Empty IDs must lead to the empty item list!
        Modes: strict,greedy,silent
        """
        if IDs == -1:
            IDs = self.ids()
        buffer = []
        for ID in IDs:
            # some dictionaries create item when it is not found,
            # so do explicit check on item's existence
            if ID not in self.__dicthash__:
                if mode=='strict':
                    raise Exception('no such ID in __dicthash__: %s'%ID)
                elif mode=='silent':
                    continue
                elif mode=='greedy':
                    buffer.append(None)
            else:
                buffer.append(self.__dicthash__[ID])
        return buffer
            
    def getfreeids(self,n):
        # Generate n IDs which don't exist in the collection.
        # TODO: optimize
        idmin = self.maxid + 1
        self.maxid += n # REMOVE THIS FROM HERE
        return list(range(idmin,idmin+n)) # gives error in Python 3 without the list() wrapper
        
    def shuffle(self,n,IDs=-1): # CHANGE NAME!!!
        """
        Shuffle the collection using the round-robin strategy.
        """
        if IDs==-1:
            IDs = self.ids()
        lst_indexes = range(len(IDs))
        shuffled_list = []
        for i in range(n):
            subindex = range(i,len(IDs),n)
            shuffled_sublist = [IDs[i] for i in subindex]
            shuffled_list.append(self.subset(shuffled_sublist))
        return shuffled_list

    def ids(self,filter='True',proc=False):
        if proc: # allows filter be much more simple to input
            filter = process_exp(filter) # experimental
        if type(filter)==str: # simple
            expr = eval('lambda var: ' + filter)
        else:
            expr = filter # advanced
        # this nice trick is taken from stack overflow:
        # http://stackoverflow.com/questions/12467570/python-way-to-speed-up-a-repeatedly-executed-eval-statement?lq=1
        id_list = []
        for ID in self.__dicthash__:
            var = self.__dicthash__[ID]
            #try: # risky
            #    flag = expr(var)
            #except KeyError:
            #    flag = False
            flag = expr(var)
            if flag:
                id_list.append(ID)
        return id_list
        
#    def keys(self):
#        # old version
#        keys = set()
#        for ID in self.__dicthash__:
#            keys = keys.union(self.__dicthash__[ID].keys())
#        keys = list(keys); keys.sort()
#        keys = tuple(keys)  
#        return keys
    
    def keys(self):
        # new version, slow but more informative
        keys = {}
        for ID in self.__dicthash__:
            for key in self.__dicthash__[ID].keys():
                if key not in keys:
                    keys[key] = 1
                else:
                    keys[key] += 1
        return keys

#    def keys(self):
#        # new version #2, slightly faster
#        keys_ = {}
#        # map
#        for ID in self.__dicthash__:
#            k = tuple(self.__dicthash__[ID].keys())
#            if k not in keys_:
#                keys_[k] = 1
#            else:
#                keys_[k] += 1
#        # reduce
#        keys = {}
#        for key_tuple in keys_:
#            for key in key_tuple:
#                if key not in keys:
#                    keys[key] = keys_[key_tuple]
#                else:
#                    keys[key] += keys_[key_tuple]
#        return keys

    def subset(self,IDs=-1): # keys must be the same as in the original collection
        if IDs==-1:
            IDs = self.ids()
        #new_coll = Collection()
        #items = [self.__dicthash__[ID] for ID in IDs]
        #new_coll.update(items,IDs)
        #new_coll.order = self.order
        #return new_coll
        new_coll = Collection()
        new_coll.__dicthash__ = {ID:self.__dicthash__[ID] for ID in IDs}
        new_coll.__dicthash__
        new_coll.order = self.order
        new_coll.maxid = max(IDs) if len(IDs)!=0 else -1
        return new_coll
        
    def slice(self,colnames=None):
        """
        Produce new collection contaning only specified colnames.
        If colnames are not supplied, then a full copy of collection
        is returned.
        """
        if type(colnames) is str:
            colnames = [colnames]
        col = Collection()
        for ID in self.__dicthash__:
            item = self.__dicthash__[ID]
            if colnames:
                item_ = {k:item[k] for k in colnames if k in item}
            else:
                item_ = {k:item[k] for k in item}
            if item_:
                col.__dicthash__[ID] = item_
        if colnames:
            col.order = colnames
        else:
            col.order = self.order.copy()
        return col
        
    def map(self,expr={}):
        """
        Map collection items to different keys and return a new collection.
        The expression "expr" should be a either a static mapping dictionary,
        or a lambda function taking a list of keys and returning a mapping dictionary.
        Example 1 (static):
            expr = {'a':'b','c':'d'}
        Example 2 (lambda):
            expr = lambda keys: ['_'+k for k in keys]
        Default mapping doesn't change key names at all.
        """
        if type(expr) is dict:
            expr_ = lambda keys: expr
        else:
            expr_ = expr
        col = Collection()
        for ID in self.__dicthash__:
            item = self.__dicthash__[ID]
            keys = list(item.keys())
            mapping = expr_(keys)
            keys_mapped = []
            keys_unmapped = []
            for k in keys:
                if k in mapping:
                    keys_mapped.append(k)
                else:
                    keys_unmapped.append(k)
            item_ = {}
            for k in keys_unmapped:
                item_[k] = item[k]
            for k in keys_mapped:
                item_[mapping[k]] = item[k]            
            if item_:
                col.__dicthash__[ID] = item_
        # handle order
        mapping = expr_(self.order)
        keys_mapped = []
        keys_unmapped = []
        for k in self.order:
            if k in mapping:
                keys_mapped.append(mapping[k])
            else:
                keys_unmapped.append(k)
        col.order = keys_unmapped + keys_mapped
        return col
        
    def cast(self,type_dict,IDs=-1):
        if IDs==-1:
            IDs = self.ids()
        nchanged = 0
        for ID in IDs:
            if ID not in self.__dicthash__:
                raise Exception('no such ID in __dicthash__: %s'%ID)
            var = self.__dicthash__[ID]
            flag_changed = False
            for col in type_dict:
                if not col in var:
                    continue
                flag_changed = True
                tp = type_dict[col]
                if col in var:
                    var[col] = tp(var[col])
            if flag_changed:
                nchanged += 1
        return {'changed':nchanged}
    
    def batch_(self,expr,IDs=-1):
        if IDs==-1:
            IDs = self.ids()
        ##### OPTIMIZE!!!!!! expr is parsed on each iteration
        for ID in IDs:
            if ID not in self.__dicthash__:
                raise Exception('no such ID in __dicthash__: %s'%ID)
            var = self.__dicthash__[ID]
            exec(expr) # very slow!!!
    
    def assign(self,par,expr,IDs=-1):
        """
        Create new parameter and assign 
        some initial value that may depend on
        other parameters within the item.
        ATTENTION: WILL BE DEPRECATED
        """
        if IDs==-1:
            IDs = self.ids()
        if type(expr)==str: # simple
            expr = eval('lambda var: ' + expr)
        for ID in IDs:
            if ID not in self.__dicthash__:
                raise Exception('no such ID in __dicthash__: %s'%ID)
            var = self.__dicthash__[ID]
            var[par] = expr(var)
        #self.__order__.append(par)
        if par not in self.order:
            self.order.append(par)
            
    def assign_(self,expr,IDs=-1): 
        """
        More flexible and convenient version of assign.
        Takes a lambda, function or callable object 
        as an input, and returns a dictionary
        of the type {'a1':val1,'a2':val2,...}
        where a1,a2,... are new/existing fields, and 
        val1, ... are values to be assigned to those fields.
        The expression takes current item as an input.
        The fields may vary depending on the current item.
        """
        if IDs==-1:
            IDs = self.ids()
        if type(expr)==str: # simple
            expr = eval('lambda var: ' + expr)
        for ID in IDs:
            if ID not in self.__dicthash__:
                raise Exception('no such ID in __dicthash__: %s'%ID)
            var = self.__dicthash__[ID]            
            vals = expr(var)
            for par in vals:
                var[par] = vals[par]
        #self.__order__ += list(dct.keys())

    def assign__(self,dct,IDs=-1):  # expr => dct
        """
        More flexible and convenient version of assign.
        New version takes a dictionary of functions 
        as an input, and returns a dictionary
        of the type {'a1':val1,'a2':val2,...}
        where a1,a2,... are new/existing fields, and 
        val1, ... are values to be assigned to those fields.
        The expression takes current item as an input.
        The fields may vary depending on the current item.
        """
        if IDs==-1:
            IDs = self.ids()
        #if type(expr)==str: # simple
        #    expr = eval('lambda var: ' + expr)
        if type(dct) is not dict:
            raise Exception('dictionary is expected at input')
        for ID in IDs:
            if ID not in self.__dicthash__:
                raise Exception('no such ID in __dicthash__: %s'%ID)
            var = self.__dicthash__[ID]            
            #vals = expr(var)
            #for par in vals:
                #var[par] = vals[par]
            for par in dct:
                var[par] = dct[par](var)
        #self.__order__ += list(dct.keys())
        
    def index(self,expr): # ex-"reform"
        """
        !!! ATTENTION !!!
        Reforms core dictionary by assigning another parameter as key.
        New key MUST be defined for all items in collection.
        Don't use this method you are not sure about consequences.
        Parameter expr can be a string or function. 
        __________________________________________
        !!! TODO: in next index implementation add 
        expressions instead of the field  name.
        __________________________________________
        """
        if type(expr) == str:
            new_id_func = eval('lambda var: var["%s"]'%expr)
            #new_id_func = eval('lambda var: %s'%expr) # this is fucking confusing, don't uncomment it anymore please
        elif type(expr) in {tuple,list}:
            new_id_func = lambda v: tuple([v[k] for k in expr])
        else:
            new_id_func = expr # user-supplied function on item
        # check uniqueness of a new index key
        new_id_vals = []
        for ID in self.__dicthash__:
            item = self.__dicthash__[ID]
            new_id_vals.append(new_id_func(item))
        if len(new_id_vals)!=len(set(new_id_vals)):
            raise Exception('new index is not unique')
        # if there is no duplicates, proceed further
        __dicthash__ = self.__dicthash__ # backup dict hash
        self.__dicthash__ = {}
        keys = list(__dicthash__.keys()) # Python 3 has special object dict_keys
        for ID in keys:
            item = __dicthash__.pop(ID)
            ID_ = new_id_func(item)
            #item['__id__'] = ID_
            self.__dicthash__[ID_] = item
        return self
        
    def get(self,ID,colname):
        """
        Get an element from an item with given ID.
        The point-reference is supported in colnames, i.e.
        "a.b" will search for parameter "b" in the object "a".
        Point-referencing can be multiple.
        """
        if ID not in self.__dicthash__:
            raise Exception('ID=%s is not in dicthash'%str(ID))        
        item = self.__dicthash__[ID]
        chain = colname.split('.')
        cur_obj = item[chain[0]]
        for e in chain[1:]:
            #cur_obj = eval('cur_obj.%s'%e) # atrocious (and slow)
            cur_obj = getattr(cur_obj,e) 
        return cur_obj
        
    def getcols(self,colnames,IDs=-1,strict=True,mode=None,
                functions=None,process=None): # get rid of "strict" argument in ver. 4.0
        """
        Extratct columns from collection.
        If parameter "strict" set to true,
        no exception handling is performed.
        The "functions" parameter is a dictionary containing
        the functions on the item. It also should be present in "colnames".
        Another update: now colname can have a properties,
        such as "col.var"
        __ID__ is a special parameter which corresponds to the local __dicthash__ ID.
        """
        # mode options: 'strict', 'silent', 'greedy'   # add this to docstring in ver. 4.0.
        if not mode: mode = 'strict' if strict else 'silent' 
        if not functions: functions = {}
        #print('%s mode'%mode)
        if IDs==-1:
            IDs = self.ids()
        if type(colnames) is str:
            colnames = [colnames]
        elif type(colnames) is not list:
            raise Exception('Column names should be either list or string')
        cols = []
        for colname in colnames:
            if type(colname) not in [str,unicode]:
                raise Exception('Column name should be a string')
            cols.append([])
        for ID in IDs:
            for i,colname in enumerate(colnames):
                if colname == '__ID__':
                    cols[i].append(ID)
                    continue                    
                try:
                    #cols[i].append(self.__dicthash__[ID][colname]) % old
                    if colname not in functions:
                        cols[i].append(self.get(ID,colname)) # "get" this should be a method of item in the next version of Jeanny
                    else:
                        cols[i].append(functions[colname](self.__dicthash__[ID]))
                except (KeyError, AttributeError) as e: 
                    if mode=='strict':
                        raise e
                    elif mode=='silent':
                        pass
                    elif mode=='greedy':
                        cols[i].append(self.__dicthash__[ID].get(colname))
                    else:
                        raise Exception('unknown mode: %s'%mode)
                    #if strict: raise e # old version with "strict" argument
        if process:
            cols = [process(col) for col in cols]
        return cols
        
    def getcol(self,colname,IDs=-1,strict=True,mode='greedy',functions=None): # get rid of "strict" argument in ver. 4.0
        """
        Wrapper for a single-column call.
        """
        colnames = [colname,]
        return self.getcols(colnames=colnames,IDs=IDs,strict=strict,mode=mode,functions=functions)[0]
        
    def splitcol(self,colname,newcols=None):
        if newcols is None:
            vals = self.getitem(next(iter(self.__dicthash__.keys())))[colname]
            newcols = [colname+'_%d'%i for i,_ in enumerate(vals)]
        for i,cname in enumerate(self.order):
            if cname==colname: self.order.pop(i)
        self.order += newcols
        for item in self.getitems():
            vals = item[colname]
            for newcol,val in zip(newcols,vals):
                item[newcol] = val
            del item[colname]
                       
    def deletecol(self,colname):
        self.deletecols(colname)
            
    def deletecols(self,colnames):
        if type(colnames) not in [list,tuple]:
            colnames = [colnames]
        for i,cname in enumerate(self.order):
            if cname in colnames: self.order.pop(i)
        for item in self.getitems():
            for colname in colnames:
                if colname in item:
                    del item[colname]
        
    def split(self,colname,vals):
        """ Split collection using sharding of column by values.
            Column 'colname' must have numeric format. """
        cols = []
        for i,val in enumerate(vals):
            if i==0:
                col = self.subset(self.ids(lambda v:v[colname]<=val))
            else:
                val_ = vals[i-1]
                col = self.subset(self.ids(lambda v:val_<v[colname]<=val))
            cols.append(col)
        col = self.subset(self.ids(lambda v:v[colname]>val))
        cols.append(col)
        return cols
        
    def tabulate(self,colnames=None,IDs=-1,mode='greedy',fmt='simple',file=None,functions=None,raw=False,floatfmt=None): # switched default to "greedy" instead of "strict"
        """
        Supported table formats are:
        
        - "plain"
        - "simple"
        - "grid"
        - "fancy_grid"
        - "pipe"
        - "orgtbl"
        - "jira"
        - "psql"
        - "rst"
        - "mediawiki"
        - "moinmoin"
        - "html"
        - "latex"
        - "latex_booktabs"
        - "textile"

        More info on usage of Tabulate can be found at 
        https://pypi.org/project/tabulate/
        """
                
        try:
            floatfmt = self.floatfmt
        except AttributeError:
            if floatfmt is None: floatfmt = 'f'
                
        if colnames==None:
            #colnames = list(self.keys().keys()) # this will prevent byg in Python3 since {}.keys() return dict_keys object instead of a list
            allkeys = list(self.keys().keys())
            colnames = self.order + list(set(allkeys)-set(self.order))
        elif type(colnames) is str:
            colnames = colnames.split()

        def in_notebook():
            # http://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook            
            """
            Returns ``True`` if the module is running in IPython kernel,
            ``False`` if in IPython shell or other Python shell.
            """
            return 'ipykernel' in sys.modules
        
        from tabulate import tabulate as tab # will make my own tabulator in the future
        data = self.getcols(colnames,IDs=IDs,mode=mode,functions=functions)  
        if file:
            with open(file,'w') as f:
                tabstring = tab(zip(*data),colnames,tablefmt=fmt,floatfmt=floatfmt)
                f.write(tabstring)
        else:
            if in_notebook():
                from IPython.core.display import display, HTML
                tabstring = tab(zip(*data),colnames,tablefmt='html',floatfmt=floatfmt)
                display(HTML(tabstring))
            else:
                tabstring = tab(zip(*data),colnames,tablefmt=fmt,floatfmt=floatfmt)
                if raw:
                    return tabstring
                else:
                    print(tabstring)
                #return TabObject(tabstring)
                
    def head(self,n=10):
        self.tabulate(IDs=self.ids()[:n])

    def tail(self,n=10):
        ids = self.ids()
        self.tabulate(IDs=ids[len(ids)-n:len(ids)])
    
    # old WORKING version
    def update(self,items,IDs=None):
        if type(items) is dict:
            items = [items]
        elif type(items) not in [list,tuple]:
            raise Exception('Items should be either list or tuple')
        if not IDs:
            IDs = self.getfreeids(len(items))
        #if type(IDs) is int:
        #    IDs = [IDs]
        elif type(IDs) is not list:
            raise Exception('Wrong IDs type: %s (expected list or integer)'%type(IDs))
        for ID,item in zip(IDs,items):
            if ID not in self.__dicthash__:
                self.__dicthash__[ID] = {}
            self.__dicthash__[ID].update(item)

    # new unreliable version
    #def update(self,items,merge=False): # this version assumes IDs are in items
    #    # if merge is True, new item is blended into existing item with the same id (if present)
    #    # if merge is False, new item overwrites existing item with the same id
    #    #if type(items) is dict:
    #    if issubclass(items.__class__,dict):
    #        items = [items]
    #    elif type(items) not in [list,tuple]:
    #        raise Exception('Items should be either list or tuple of dict children')
    #    IDs = self.getfreeids(len(items))
    #    for item in items:
    #        if '__id__' not in item:
    #            ID = IDs.pop(0)
    #            item['__id__'] = ID
    #        else:
    #            ID = item['__id__']
    #        if merge:
    #            if ID not in self.__dicthash__:
    #                self.__dicthash__[ID] = {}
    #            self.__dicthash__[ID].update(item)
    #        else:
    #            self.__dicthash__[ID] = item
        
    def delete(self,IDs):
        for ID in IDs:
            del self.__dicthash__[ID]
        
    def group(self,expr): # TODO: add process_exp
        buffer = {}
        if type(expr)==str:
            expr_ = eval('lambda var: var["' + expr + '"]') # simple
        elif type(expr) in {list,tuple}:
            expr_ = lambda v: tuple([v[k] for k in expr])
        else:
            expr_ = expr
        for ID in self.__dicthash__:
            var = self.__dicthash__[ID] 
            group_value = expr_(var)
            if group_value not in buffer:
                buffer[group_value] = []
            buffer[group_value].append(ID)
        return buffer
        
    def stat(self,keyname,grpi,valname,map=None,reduce=None,plain=False):  # Taken from Jeanny v.4 with some changes
        """
        Calculate function on index values.
        User must provide:
            -> group index (grpi)
            -> mapper and reducer functions.
        MAPPER: item->value (can be scalar or vector)
        REDUCER: item_dict_array->value (can be scalar or vector)
        Flat: True - return plain stat index, False - return Collection
        """
        if map is None: map = lambda v: v
        if reduce is None: reduce = lambda ee: ee
        group_buffer = grpi
        stat_index = {}
        for index_id in group_buffer:
            ids = group_buffer[index_id]
            items = self.getitems(ids)
            map_values = [map(item) for item in items]
            reduce_value = reduce(map_values)
            stat_index[index_id] = reduce_value
        if plain: # return only stat index
            return stat_index 
        else: # return index-based collection
            col = Collection()
            col.__dicthash__ = {
                key:{keyname:key,valname:stat_index[key]} \
                for key in stat_index}
            col.order = [keyname,valname]
            return col

    def sort(self,colnames,IDs=-1,strict=True,mode='greedy',functions=None): # switched default to "greedy" instead of "strict"
        """
        Return permutation of the input IDs.
        """
        if IDs==-1:
            IDs = self.ids()
        vals = self.getcols(colnames=colnames,IDs=IDs,strict=strict,mode=mode,functions=functions)
        vals = [list(e)+[id] for e,id in zip(zip(*vals),IDs)]
        IDs_res = [e[-1] for e in sorted(vals)]
        return IDs_res

    def join(self,key,col,colnames=None,prefix='',strict=True):
        """
        Join column set of external collection (colnames)
        to self using the key, assuming the following conditions:
        1) Self must contain the column named as key.
        2) Col must have the corresponding values of key in its index.
        Key can be either lambda function on item, or a field name
        If strict==False, then ignore if colname is not in col item.
        """
        if type(key)==str:
            key_ = lambda v: v[key]
        elif type(key) in {tuple,list}:
            key_ = lambda v: tuple([v[k] for k in key])
        else:
            key_ = key # expecting lambda func, returning col's index values
        if colnames is None:
            colnames_empty = True; order_set = set()
        else:
            colnames_empty = False        
        for item in self.getitems():
            k_ = key_(item)
            if k_ not in col.__dicthash__:
                continue # skip to next iteration if key is not found in external col
            external_item = col.__dicthash__[k_]            
            if colnames_empty:
                colnames = list(external_item.keys())
            for colname in colnames:
                colname_ = prefix+colname
                if colname_ in item:
                    raise Exception('Collection item already has "%s" field'%colname)
                if not strict and colname not in external_item:
                    continue
                item[colname_] = external_item[colname]
                if colnames_empty: order_set.add(colname_)
        if colnames_empty: 
            self.order += order_set
        else:
            self.order += [prefix+colname for colname in colnames]
        
    # =======================================================
    # =================== UNROLL/UNWIND =====================
    # =======================================================
    def unroll(self,keys,IDs=-1):
        """
        Perform the unrolling of the iterable field(s).
        For example, for {'a':[1,2,3],'b':[10,20]} this will give a new collection:
        {'a':1,'b':10}
        {'a':1,'b':20}
        {'a':2,'b':10}
        {'a':2,'b':20}
        {'a':3,'b':10}
        {'a':3,'b':20}        
        The items not containing the keys from the list will be intact. 
        This method does nearly the same as the "unwind" in MongoDB         
        """
        if type(keys) not in [list,tuple]:
            keys = [keys]
        if IDs==-1:
            IDs = self.ids()
        col = Collection(); col.order = self.order
        def to_list(val):
            if type(val) in [str,unicode]: # string is a special case
                return [val]
            elif type(val) in [list,tuple]: # list and tuple don't need further conversion
                return val
            else: # other cases; TODO: should add iterables separately in the future
                return [val]
        for item in self.getitems(IDs):
            # get the keys from the list which are present in the current item
            active_keys = []
            for key in keys:
                if key in item: active_keys.append(key)
            if not active_keys: # no keys at all
                col.update(item)
            else: # some keys have been found
                for vals in it.product(*[to_list(item[key]) for key in active_keys]):
                    new_item = item.copy()
                    #new_item.update({key:val for key,val in zip(active_keys,vals)}) # this doesn't work in earlier Python versions
                    for key,val in zip(active_keys,vals):
                        new_item[key] = val
                    col.update(new_item)
        return col

    # =======================================================
    # ======================= CSV ===========================
    # =======================================================
    
    def import_csv(self,filename,delimiter=';',quotechar='"',header=None,duck=True): # old_version
        """
        Reads csv-formatted files in more or less robust way.
        Includes avoiding many parsing errors due to "illegal"
        usage of delimiters and quotes.
        """
        # TODO: use csv.Sniffer to deduce the format automatically
        items = []
        with open(filename,'r') as f:
            reader = csv.reader(f,delimiter=delimiter,quotechar=quotechar)
            if not header:                
                colnames = next(reader) # take the first line as header (even if it's really absent)
            else:
                colnames = header            
            nitems = 0
            for vals in reader:
                nitems += 1
                item = {}
                for colname,val in zip(colnames,vals):
                    if val in [None,'']: continue
                    if duck: # duck typing
                        try:
                            val = int(val)
                        except ValueError as e:
                            try:
                                val = float(val)
                            except ValueError as e:
                                if val.strip().lower() in ['f','false','.false.']:
                                    val = False
                                elif val.strip().lower() in ['t','true','.true.']:
                                    val = True
                                else:
                                    pass
                    if type(val) in [str,unicode]: # f..king encoding problems 
                        try:
                            unicode(val)
                        except UnicodeDecodeError:
                            raise Exception('encoding/decoding problems with %s'%val)
                    #if val: item[colname] = val
                    item[colname] = val
                items.append(item)
        self.clear()
        self.update(items)
        self.order = colnames
        return {'nitems':nitems}
        
    def import_csv_(self,filename): # new_version
        """
        Reads csv-formatted files in more or less robust way.
        Includes avoiding many parsing errors due to "illegal"
        usage of delimiters and quotes.
        """
        with open(filename, newline='') as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(10000))
            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)
            header = next(reader)
            items = []
            for vals in reader:
                item = {key:val for key,val in zip(header,vals)}
                items.append(item)
            self.clear()
            self.update(items)
            self.order = header
        
    def export_csv(self,filename,delimiter=';',quotechar='"',order=[]):
        """
        Writes csv-formatted files in more or less robust way.
        Includes avoiding many parsing errors due to "illegal"
        usage of delimiters and quotes.
        Order contains key names which will go first.
        Order saves from reordering columns in Excel each time 
        the CSV file is generated.
        """
        if not order: order = self.order
        keys = self.keys(); 
        header = [key for key in order] + \
                 [key for key in keys if key not in order] # ordered keys must go first
        with open(filename,'w') as f:
            writer = csv.writer(f,delimiter=delimiter,
                  quotechar=quotechar,lineterminator='\n',
                  quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for ID in self.__dicthash__:
                item = self.__dicthash__[ID]
                vals = []
                for colname in header:
                    if colname in item and item[colname] is not None:
                        vals.append(unicode(item[colname]))
                    else:
                        vals.append('')
                writer.writerow(vals)
                
    # =======================================================
    # ======================= xlsx ==========================
    # =======================================================
    
    def import_xlsx(self,filename):
        """
        Read in the table stored in the Excel file.
        The upper row must contain the column names.
        """
        from openpyxl import Workbook,load_workbook

        # read workbook
        #wb = load_workbook(filename,use_iterators=True) # Python 3: TypeError: load_workbook() got an unexpected keyword argument 'use_iterators'
        wb = load_workbook(filename)
        sheet = wb.worksheets[0]
        rowlist = list(sheet) # can be inefficient when the file is large; better to use iterators in this case

        # get row and column count
        #http://stackoverflow.com/questions/13377793/is-it-possible-to-get-an-excel-documents-row-count-without-loading-the-entire-d
        #row_count = sheet.max_row
        #column_count = sheet.max_column
    
        # get header, i.e. names of the columns
        header = [cell.value for cell in rowlist[0]]
    
        # fill collection with data
        items = []
        nitems = 0
        for row in rowlist[1:]:
            values = [cell.value for cell in row]
            nitems += 1
            item = {}
            for colname,val in zip(header,values):
                #if val: item[colname] = val # !!!!! BUG: if val==0, it will not be recorded!!!
                if val is not None: item[colname] = val 
            #items.append(item)
            # !!!!!! BUG IN EXCEL/openpyxl: if rows are deleted in Excel, they are still there in the sheet, which will result in many empty items
            if item: items.append(item) 

        self.clear()
        self.update(items)
        return {'nitems':nitems}
        
    # =======================================================
    # ===================== Folder ==========================
    # =======================================================
            
    def import_folder(self,dirname,regex='\.json$'):
        filenames = scanfiles(dirname,regex)
        items = []
        for filename in filenames:
            with open(os.path.join(dirname,filename)) as f:
                try:
                    item = json.load(f)
                except:
                    print('ERROR: %s'%filename)
                    raise
                #if FILENAME_ID in item:
                #    raise Exception('%s has a key %s'%(filenames,FILENAME_ID))
                item[FILENAME_ID] = filename
                items.append(item)
        self.clear()
        self.update(items)
    
    def export_folder(self,dirname,ext='json',default=json_serial):
        """
        Updated version with integrity checking
        to prevent overwriting items in the folder
        in the case when there are similar file names.
        """
        if not os.path.isdir(dirname):
            #os.mkdir(dirname) # this doesn't work if there are sub-folders
            os.makedirs(dirname)
        # prepare file name index
        FILENAMES = {}
        for ID in self.__dicthash__:
            item = self.__dicthash__[ID]
            if FILENAME_ID in item:
                filename = item[FILENAME_ID]
                if ext:
                    filename,_ = os.path.splitext(filename)
                    filename += ext if ext[0]=='.' else '.'+ext
            elif ITEM_ID in item:
                filename = item[ITEM_ID] 
                filename += ext if ext[0]=='.' else '.'+ext
            else:
                #filename = str(uuid.UUID(bytes=os.urandom(16),version=4)) 
                filename = uuid() 
                filename += ext if ext[0]=='.' else '.'+ext
            FILENAMES[ID] = filename
        # check this index for integrity
        #if len(set(FILENAMES.keys()))!=len(FILENAMES.keys()):# BUG!!!!!!
        if len(set(FILENAMES.values()))!=len(FILENAMES.values()):
            raise Exception('%s index is not unique'%FILENAME_ID)
        # if everything is OK save the collection
        for ID in self.__dicthash__:
            item = self.__dicthash__[ID].copy()
            if FILENAME_ID in item:
                del item[FILENAME_ID]
            filename = FILENAMES[ID]
            with open(os.path.join(dirname,filename),'w') as f:
                #json.dump(item,f,indent=2) # default "dumper"
                json.dump(item,f,indent=2,default=default) # custom "dumper"
                     
    def update_folder(self,dirname,regex=''):
        """
        Update the Jeanny folder collection with self.
        !!! BOTH COLLECTIONS MUST HAVE THE SAME IDS !!!
        !!! ONE MUST USE THE FILENAME_ID FOR THESE PARAMETERS !!!
        """
        dest_col = Collection()
        if os.path.isdir(dirname):
            dest_col.import_folder(dirname,regex)
            dest_col.index('var["%s"]'%FILENAME_ID)
        for item in self.getitems():
            FILENAME = item[FILENAME_ID]
            if FILENAME in dest_col.__dicthash__.keys():
                dest_item = dest_col.getitem(FILENAME)
                dest_item.update(item)
            else:
                dest_col.update(item)
        dest_col.export_folder(dirname)

    # =======================================================
    # ===================== JSON List =======================
    # =======================================================
        
    def import_json_list(self,filename,id=None):
        with open(filename,'r') as f:
            buffer = json.load(f)
        items = []
        for item in buffer:
            items.append(item)
        self.clear()
        self.update(items)
        
    def export_json_list(self,filename,default=json_serial):
        buffer = self.getitems(self.ids())
        with open(filename,'w') as f:
            #json.dump(buffer,f,indent=2) # default "dumper"
            json.dump(buffer,f,indent=2,default=default) # custom "dumper"

    # =======================================================
    # ===================== JSON Dicthash ===================
    # =======================================================
                    
    def export_json_dicthash(self,filename,default=json_serial):
        with open(filename,'w') as f:
            #json.dump(self.__dicthash__,f,indent=2) # default "dumper"
            json.dump(self.__dicthash__,f,indent=2,default=default) # custom "dumper"
            
    # =======================================================
    # ============= Fixcol/Parse from string ================
    # =======================================================
            
    def import_fixcol(self,filename,ignore=True,substitute=None):
        """
        Create collection from the specially formatted column-fixed file.
        THe file must be supplied in the following format (type can be omitted):
        
        //HEADER
        0 Column0 Type0
        1 Column1 Type1
        ...
        N ColumnN TypeN
        
        //DATA
        0___1___2____.....N______
        .... data goes here ....
        
        Comments are marked with hashtag (#) and ignored.
        
        If ignore set to False, exception is thrown at any conversion problems.
        """               
        TYPES = {'float':float,'int':int,'str':str}
        
        f = open(filename)
    
        # Search for //HEADER section.    
        for line in f:
            if '//HEADER' in line: break
    
        # Scan //HEADER section.     
        HEAD = {}        
        for line in f:
            line = line.strip()
            if not line: continue
            if line[0]=='#': continue
            if '//DATA' in line: break
            vals = [_ for _ in line.split() if _]
            token = vals[0]
            if token in HEAD:
                raise Exception('ERROR: duplicate key was found: %s'%vals[0])
            vtype = TYPES[vals[2]] if len(vals)>2 else str           
            HEAD[token] = {}
            HEAD[token]['token'] = token
            HEAD[token]['name'] = vals[1]
            HEAD[token]['type'] = vtype # vtype
                        
        # Get tokenized mark-up.
        for line in f:
            widths = line.rstrip(); break # readline doesn't work because of the "Mixing iteration and read methods"
        matches = re.finditer('([^_]+_*)',widths)
        tokens = []; names = []
        for match in matches:
            i_start = match.start()
            i_end = match.end()
            token = re.sub('_','',widths[i_start:i_end])
            if token not in HEAD: continue                
            tokens.append(token)
            names.append(HEAD[token]['name'])
            HEAD[token]['i_start'] = i_start
            HEAD[token]['i_end'] = i_end
        #markup = re.findall('([^_]+_*)',widths) # doesn't give indexes
        
        # Scan //DATA section.     
        items = []
        for line in f:
            if line.strip()=='': continue
            if line.lstrip()[0]=='#': continue
            line = line.rstrip()
            #item = {HEAD[token]['name']:HEAD[token]['type'](line[HEAD[token]['i_start']:HEAD[token]['i_end']]) for token in HEAD} # doesn't work in earlier versions of Python
            item = {}
            for token in HEAD:
                try:
                    buf = line[HEAD[token]['i_start']:HEAD[token]['i_end']]
                    val = HEAD[token]['type'](buf)
                except ValueError as e:
                    if not ignore:
                        raise Exception(e)
                    else:
                        val = substitute
                item[HEAD[token]['name']] = val
            items.append(item) 
        self.clear()
        self.setorder(names)
        self.update(items)    
    
    def export_fixcol(self,filename):
        """ 
            ATTENTION:
            This was done to be used ONLY if a column-fixed format is mandatory!!!
            This function is buggy and will cause non-reversible changes in the 
            the string values of teh items.
            Use other export formats to achieve stability. 
        """
        
        # conversion to string accounting for None values
        def to_str(val):
            if val is not None:
                return str(val)
            else:
                return ''
        # get order
        order = self.order.copy()
        order += list( set(self.keys())-set(order) )
        # deduce types from the collection
        checked = set()
        types = {}
        ids = self.ids()
        for id_ in ids:
            item = self.getitem(id_)
            if not set(order)-checked: break
            for colname in order:
                if colname in item and item[colname] is not None:
                    types[colname] = type(item[colname])
                    checked.add(colname)
        # if items are not present, assign str type for them
        for colname in set(order)-checked:
            types[colname] = str
        with open(filename,'w') as f:
            f.write('//HEADER\n')
            # make a header
            tokens = string.digits+string.ascii_uppercase+string.ascii_lowercase
            tokens = tokens[:len(order)]
            for token,colname in zip(tokens,order):
                f.write('%s %s %s\n'%(token,colname,types[colname].__name__))
            f.write('\n//DATA\n')
            # get columns and find widths
            COLS = self.getcols(order)
            COLS_STR = [[to_str(e) for e in col] for col in COLS]
            get_width = lambda col: max([len(s) for s in col])
            widths = [get_width(col) for col in COLS_STR]
            # write tokenized header
            dw = 3 # gap between columns
            for token,width in zip(tokens,widths):
                f.write(token+'_'*(width-1+dw))
            f.write('\n')
            # write the content, do conversion checks
            ncols = len(COLS_STR)
            for i in range(len(ids)):
                for j in range(ncols):
                    type_ = types[order[j]]
                    strval = COLS_STR[j][i]
                    trueval = COLS[j][i]
                    if trueval is not None: 
                        # this should signalize if there are conversion problems
                        if trueval!=type_(strval):
                            raise Exception('conversion error: ',trueval,type_(strval)) 
                    width = widths[j]
                    f.write('%%%ds'%(width+dw)%strval)
                f.write('\n')
    
    # =======================================================
    # ============= XSCDB/HAPI2.0 STUFF =====================
    # =======================================================
    
    def xscdb_lookup_molecule(self,colname,IDs=-1):
        import xscdb
        if not xscdb.VARSPACE['session']:
            xscdb.start()
        lookup = lambda al: xscdb.query(xscdb.Molecule).\
                            join(xscdb.MoleculeAlias).\
                            filter(xscdb.MoleculeAlias.alias.like(al)).first()
        if IDs==-1: 
            IDs = self.ids()
        for ID in IDs:
            v = self.__dicthash__[ID]
            altypes = ['name','csid','cas','acronym']
            res = {}
            for altype in altypes:                
                if altype in v:
                    dbitem = lookup(v[altype])
                    if dbitem is not None: res[altype] = dbitem
            vals = set(res.values())
            if len(vals)>1:
                #print('WARNING: more than one entrie found for %s: %s'%\
                #  ({altype:v[altype] for altype in altypes if altype in v},res)) # doesn't work in earlier versions of Python
                aa = {}
                for altype in altypes:
                    if altype in v: aa[altype] = v[altype]
                print('WARNING: more than one entrie found for %s: %s'%(aa,res))
                v[colname] = tuple(vals)
            else:
                v[colname] = tuple(vals)[0]
                
    # =======================================================
    # ============= Checksums and integrity =================
    # =======================================================
    
    def md5(self,v):# Jeanny4: this must belong to item, not to collection
        return hashlib.md5('%s'%[v[key] for key in sorted(v.keys())]).hexdigest()

    # =======================================================
    # ============= Other unfinished stuff... ===============
    # =======================================================
        
    def import_binary(self,filename):
        pass
        
    def export_binary(self,filename):
        pass

class Tree: # can a collection do that ??????????
    """
    A collection tree used especially for the cluster parallelized computations
    (temporary implementation).
    """
    
    def __init__(self,folders=None,type='folder',regex='\.json'):   
        self.__cols__ = []
        if folders is not None:
            self.read(folders,type=type,regex=regex)
            
    def __repr__(self):
        res = ',\n'.join([str(col) for col in self.__cols__])
        return res
        
    def read(self,folders,type='folder',regex='\.json'):
        for path in folders:
            print('reading collection from %s'%path)
            col = Collection(path=path,type=type,regex=regex)
            self.__cols__.append(col)
                
    def write(self,folders=None,type=None):
        # check pathes first
        for col in self.__cols__:
            if col.__path__ in {'',None}:
                raise Exception('Path should be non-empty for col %s'%col)
        # get folders
        if folders is None:
            folders = []
            for col in self.__cols__:
                folders.append(col.__path__)
        for col,path in zip(self.__cols__,folders):
            print('exporting %s to %s'%(col,path))
            col.export(path=path,type=type)
        
    def assign(self,par,expr):
        for col in self.__cols__:
            col.assign(par,expr)
        
    def assign_(self,expr):
        for col in self.__cols__:
            col.assign_(expr)
            
    def delete(self,**argv):
        for col in self.__cols__:
            col.delete(**argv)
            
    def subset(self,**argv):
        t = Tree()        
        for col in self.__cols__:
            t.__cols__.append(col.subset(**argv))
        return t
            
    def union(self):
        res = Collection()
        for col in self.__cols__:
            res.update(col.getitems())
        return res

class Database:
    """
    A class describing the set of heterogenous collections  
    interconnected with each other.
    """
    
    REGISTRY_NAME = '__registry__.csv'
    RELATIONS_NAME = '__relations__.csv'
    
    def __init__(self,root):
        self.__root__ = root
        # import registry
        self.__registry__ = self.__import_if_exists__(self.REGISTRY_NAME)
        self.__registry__.index('name')
        # import relations
        self.__relations__ = self.__import_if_exists__(self.RELATIONS_NAME)
        # import collections
        for item in self.__registry__.getitems():
            name = item['name']
            path = os.path.join(self.REGISTRY_NAME,name)
            col = import_csv(path)
            
    def __import_if_exists__(self,path):
        root = self.__root__
        path = os.path.join(root,path)
        if os.path.isdir(root):
            if not os.path.isfile(path):
                raise Exception('Bad database format: cannot find %s'%path)
            col = import_csv(path)
        else:
            col = Collection()
        return col
        
    def create_collection(self,collection_name):
        assert collection_name!=self.REGISTRY_NAME
        if collection_name in self.__registry__.__dicthash__:
            raise Exception('Collection "%s" already exists'%\
                collection_name)
        col = Collection()
        self.__registry__.__dicthash__[collection_name] = {
            'name':collection_name,'col':col}
        return col
        
    def get_collection(self,collection_name):
        return self.__registry__.__dicthash__[collection_name]
        
    def __getitem__(self,collection_name):
        return self.get_collection(collection_name)
        
    def add_collection(self,col,collection_name):
        assert collection_name!=self.REGISTRY_NAME
        if collection_name in self.__registry__.__dicthash__:
            raise Exception('Collection "%s" already exists'%\
                collection_name)
        self.__registry__.__dicthash__[collection_name] = {
            'name':collection_name,'col':col}      

    def drop_collection(self,collection_name):
        del self.__registry__.__dicthash__[collection_name]
        
    # TODO !!!
    def join(self,name1,name2,key,key2=None,inner=True):
        # get join index
        col1 = self.get_collection(name1)
        col2 = self.get_collection(name2)
        join_index = create_join_index(col1,col2,key,key2,inner)
        # get column naming scheme
        keys1 = col1.keys()
        keys2 = col2.keys()
        keys_common = set(keys1).intersection(keys2)
        names_dict = lambda name,keys: \
            {k:(k if k not in keys_common else '%s.%s'%(name,k)) for k in keys}
        names_dict_1 = names_dict(name1,keys1)
        names_dict_2 = names_dict(name2,keys2)
        order1 = col1.order
        order2 = col2.order
        ordered_keys = lambda order,keys: list(order1)+list(set(keys1)-order1)
        keys_order_1 = ordered_keys(order1,keys1)
        keys_order_2 = ordered_keys(order2,keys2)
        names_order_1 = [names_dict_1[k] for k in keys_order_1]
        names_order_2 = [names_dict_2[k] for k in keys_order_2]
        # create joined collection using the naming scheme
        col = Collection()
        col.order = names_order_1 + names_order_2
        for id1,id2 in join_index:
            item = {}
            if id1 is not None:
                item1 = col1.getitem(id1)
                for c in item1:
                    c_ = names_dict_1(c)
                    item[c_] = item1[c]
            if id2 is not None:
                item2 = col2.getitem(id2)
                for c in item2:
                    c_ = names_dict_2(c)
                    item[c_] = item2[c]
            col.update(item)
        # return the resulting collection
        return col
        
    def save(self):
        # save registry
        registry_path = os.path.join(self.__root__,self.REGISTRY_NAME)
        if not os.path.exists(self.__root__):
            os.mkdir(self.__root__)
        reg = Collection()
        reg.update([{'name':v['name']} for v in self.__registry__.getitems()])
        reg.export_csv(registry_path)
        # save collections
        for item in self.__registry__.getitems():
            col = item['col']
            name = item['name']
            path = os.path.join(self.REGISTRY_NAME,name)
            col.export_csv(path)
            
    def __repr__(self):
        return 'Database("%s")'%self.__root__
            
class JobManager:
    """
        ncores => number of CPUs
        nnodes=1 => number of nodes
        mempcore=1500 => amount of RAM per core
        name='calc' => default job name
        command=None => command to launch
        walltime='24' => walltime in hours
    """
    def __init__(self,ncores=1,nnodes=1,mempcore=1500,
        name='calc',command=None,walltime='24'):
        self.ncores = ncores
        self.nnodes = nnodes
        self.mempcore = mempcore
        self.name = name
        self.command = command
        self.walltime = walltime
            
class JobManagerSGE(JobManager):
    template = """# /bin/sh 
# ----------------Parameters---------------------- #
#$ -S /bin/sh
#$ -pe mthread {ncores}
#$ -l s_cpu={walltime}:00:00
#$ -l mres={mempcore}M
#$ -cwd
#$ -j y
#$ -N {name}
#$ -o {name}.log
#$ -m bea
#
# ----------------Modules------------------------- #
#module load tools/python2.6-x
module load opt-python
module load intel

source ~/.bashrc
#
# ----------------Your Commands------------------- #
#
echo + `date` job $JOB_NAME started in $QUEUE with jobID=$JOB_ID on $HOSTNAME
echo + NSLOTS = $NSLOTS
#
{command}
#
echo = `date` job $JOB_NAME done
    """
    def __repr__(self):
        return self.template.format(
            ncores=self.ncores,
            nnodes=self.nnodes,
            mempcore=self.mempcore,
            name=self.name,
            command=self.command,
            walltime=self.walltime)
            
class JobManagerSlurm(JobManager):
    def __repr__(self):
        pass

            
# ==============================
# SUPPLEMENTARY HELPER FUNCTIONS
# ==============================

# get names of all files in a given folder
def get_filenames(dirname):
    filenames = [entry for entry in os.listdir(dirname) 
                 if os.path.isfile(os.path.join(dirname,entry))]
    return filenames
    
def get_dirnames(dirname):
    dirnames = [entry for entry in os.listdir(dirname) 
                 if os.path.isdir(os.path.join(dirname,entry))]
    return dirnames

# filter string according to supplied regular expression (PCRE)
def filterstr(lst,regex):
    return [entry for entry in lst if re.search(regex,entry)]

# scan folder for files which obey the given regular expression (PCRE)
def scanfiles(dirname='./',regex=''):
    return filterstr(get_filenames(dirname),regex)
scandir = scanfiles # BACKWARDS COMPATIBILITY!!
    
# scan folder for sub-folders which obey the given regular expression (PCRE)
def scandirs(dirname='./',regex=''):
    return filterstr(get_dirnames(dirname),regex)

def copyfile(srcdir,srcnames,destdir,destnames=None):
    if not destnames: destnames = srcnames
    for srname,destname in zip(srcnames,destnames):
        # unlike copy(), copy2() retains file attributes
        shutil.copy2(os.path.join(srcdir,srname),
                    os.path.join(destdir,destname))
    
# convert old HAPI-formatted table to Collection    
def collect_hapi(LOCAL_TABLE_CACHE,TableName):
    lines = Collection()
    for i in range(LOCAL_TABLE_CACHE[TableName]['header']['number_of_rows']):
        line = {}
        for par in LOCAL_TABLE_CACHE[TableName]['data'].keys():
            line[par] = LOCAL_TABLE_CACHE[TableName]['data'][par][i]
        lines.update(line)
    return lines
    
# ATTENTION!!! BETTER VERSIONS FOR FUNCTIONS FOR WORKING WITH .PAR HITRAN FORMAT 
# ARE GIVEN IN D:\work\Activities\HAPI\EXCEL_INTERFACE\dotpar\dotpar_converter.py
    
# import HITRAN .par file into a collection
"""
-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
par_line                                                                                                                                                         |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
281 0.000001901 1.298E-36 1.637E-25.05940.103  672.98580.580.000000      0 0 0 0        0 0 0 0   12  6 0A-      12  6 0A+     935430 5 4 2 2 1 0   200.0  200.0 |
281 0.000002071 2.913E-35 6.874E-25.07050.105   48.61680.670.000000      0 0 0 0        0 0 0 0    3  3 0A-       3  3 0A+     945430 5 4 2 2 1 0    56.0   56.0 |
281 0.000005174 5.096E-36 2.830E-24.05620.101  787.77310.570.000000      0 0 0 0        0 0 0 0   13  6 0A-      13  6 0A+     935430 5 4 2 2 1 0   216.0  216.0 |
281 0.000012976 1.636E-35 3.866E-23.05300.100  911.20010.560.000000      0 0 0 0        0 0 0 0   14  6 0A-      14  6 0A+     935430 5 4 2 2 1 0   232.0  232.0 |
281 0.000014477 9.247E-34 1.411E-22.07300.109   84.21490.660.000000      0 0 0 0        0 0 0 0    4  3 0A-       4  3 0A+     945430 5 4 2 2 1 0    72.0   72.0 |
281 0.000030368 4.412E-35 4.336E-22.04990.099 1043.22630.550.000000      0 0 0 0        0 0 0 0   15  6 0A-      15  6 0A+     935430 5 4 2 2 1 0   248.0  248.0 |
281 0.000057843 9.690E-33 5.998E-21.07280.109  128.68900.650.000000      0 0 0 0        0 0 0 0    5  3 0A-       5  3 0A+     945430 5 4 2 2 1 0    88.0   88.0 |
281 0.000067033 1.019E-34 4.113E-21.04690.097 1183.80910.540.000000      0 0 0 0        0 0 0 0   16  6 0A-      16  6 0A+     935430 5 4 2 2 1 0   264.0  264.0 |
281 0.000140651 2.048E-34 3.375E-20.04390.096 1332.90370.530.000000      0 0 0 0        0 0 0 0   17  6 0A-      17  6 0A+     935430 5 4 2 2 1 0   280.0  280.0 |
281 0.000173303 5.666E-32 1.152E-19.07170.109  182.02350.640.000000      0 0 0 0        0 0 0 0    6  3 0A-       6  3 0A+     945430 5 4 2 2 1 0   104.0  104.0 |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
par_line                                                                                                                                                         |
-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
281 0.000282342 3.628E-34 2.442E-19.04100.094 1490.46320.520.000000      0 0 0 0        0 0 0 0   18  6 0A-      18  6 0A+     835430 5 4 2 2 1 0   296.0  296.0 |
281 0.000545084 5.719E-34 1.580E-18.03820.092 1656.43890.510.000000      0 0 0 0        0 0 0 0   19  6 0A-      19  6 0A+     835430 5 4 2 2 1 0   312.0  312.0 |
281 0.003514511 3.319E-30 3.669E-16.06150.104  483.54750.600.000000      0 0 0 0        0 0 0 0   10  3 0A-      10  3 0A+     845430 5 4 2 2 1 0   168.0  168.0 |
281 0.006136062 5.752E-30 1.627E-15.05840.103  580.84530.590.000000      0 0 0 0        0 0 0 0   11  3 0A-      11  3 0A+     835430 5 4 2 2 1 0   184.0  184.0 |
281 0.010200974 8.732E-30 6.323E-15.05540.102  686.84820.580.000000      0 0 0 0        0 0 0 0   12  3 0A-      12  3 0A+     835430 5 4 2 2 1 0   200.0  200.0 |
281  17.8054720 9.543E-22 2.333E-04.07820.114    8.90430.690.000000      0 0 0 0        0 0 0 0    2  0 0A+       1  0 0A+     845430 5 4 2 2 1 0    40.0   24.0 |
281  17.8061608 7.176E-22 3.500E-04.07960.113    8.37110.690.000000      0 0 0 0        0 0 0 0    2  1 0E        1  1 0E      845430 5 4 2 2 1 0    20.0   12.0 |
281  26.7003452 2.890E-21 8.428E-04.07420.112   26.70980.680.000000      0 0 0 0        0 0 0 0    3  0 0A+       2  0 0A+     845430 5 4 2 2 1 0    56.0   40.0 |
281  26.7013756 2.575E-21 1.498E-03.07580.113   26.17730.680.000000      0 0 0 0        0 0 0 0    3  1 0E        2  1 0E      845430 5 4 2 2 1 0    28.0   20.0 |
281  26.7044710 1.623E-21 9.371E-04.07550.107   24.57810.680.000000      0 0 0 0        0 0 0 0    3  2 0E        2  2 0E      845430 5 4 2 2 1 0    28.0   20.0 |
+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
 11    0.072059 2.043E-30 5.088E-12.09190.391 1922.82910.760.003700          0 1 0          0 1 0  4  2  2        5  1  5      5545533321287120 7     9.0   11.0
 11    0.900086 5.783E-35 1.965E-08.07990.352 5613.36960.57-.002400          0 3 0          0 3 0  8  2  7        7  3  4      4442434432287122 8    51.0   45.0
 11    0.895092 3.600E-28 8.314E-09.08010.412 2129.59910.75-.000300          0 1 0          0 1 0  5  3  2        4  4  1      5546633321287120 8    33.0   27.0
 11    0.865759 5.104E-35 2.755E-08.08180.352 5435.41210.680.005300          0 3 0          0 3 0  6  3  3        7  2  6      4445534432257120 7    13.0   15.0
 11    0.768939 7.600E-37 1.894E-13.07820.434 4030.06980.690.000800          1 0 0          0 0 1  4  3  2        3  3  1      4342434432257122 7    27.0   21.0
 11    0.766500 1.393E-37 1.714E-08.05840.298 6655.62940.51-.009100          0 3 0          0 3 0  9  6  4       10  5  5      3342434432297122 9    19.0   21.0
""";

def dotpar_item_to_list(item):
    elements = ('molec_id','local_iso_id','nu','sw','a','gamma_air','gamma_self','elower','n_air','delta_air',
    'global_upper_quanta','global_lower_quanta','local_upper_quanta','local_lower_quanta','ierr','iref','gp','gpp')
    lst = [item[e] for e in elements]
    return lst

def load_dotpar(line):
    """
    Get the "raw" .par line on input
    and output the dictionary of parameters.
    """
    item = dict(
        molec_id             = int(   line[  0:  2] ),
        local_iso_id         = int(   line[  2:  3] ),
        nu                   = float( line[  3: 15] ),
        sw                   = float( line[ 15: 25] ),
        a                    = float( line[ 25: 35] ),
        gamma_air            = float( line[ 35: 40] ),
        gamma_self           = float( line[ 40: 45] ),
        elower               = float( line[ 45: 55] ),
        n_air                = float( line[ 55: 59] ),
        delta_air            = float( line[ 59: 67] ),
        global_upper_quanta  = str(   line[ 67: 82] ),
        global_lower_quanta  = str(   line[ 82: 97] ),
        local_upper_quanta   = str(   line[ 97:112] ),
        local_lower_quanta   = str(   line[112:127] ),        
        ierr                 = str(   line[127:133] ),        
        iref                 = str(   line[133:145] ),        
        gp                   = float( line[145:153] ),
        gpp                  = float( line[153:160] ),  
    )   
    return item
        
def import_dotpar(filename):
    col = Collection()
    with open(filename) as f:
        for line in f:
            item = load_dotpar(line)
            col.update(item)
    col.order = list(item.keys())
    return col

def import_fixcol(filename):
    col = Collection()
    col.import_fixcol(filename)
    return col

def import_csv(filename):
    col = Collection()
    col.import_csv(filename)
    return col

def export_to_hapi_cache(col,table_name,LOCAL_TABLE_CACHE,HITRAN_DEFAULT_HEADER):
    def append_par(table,parname,parvalue):
        if parname not in table:
            table[parname] = [parvalue]
        else:
            table[parname].append(parvalue)
    LOCAL_TABLE_CACHE[table_name] = {'data':{},'header':HITRAN_DEFAULT_HEADER}    
    for item in col.getitems():
        for key in item:
            append_par(LOCAL_TABLE_CACHE[table_name]['data'],key,item[key])

## export HITRAN .par file from a collection
#def export_dotpar(col,filename):
#    with open(filename,'w') as f:
#        for id in col.ids():
#            item = col.__dicthash__[id] # not compatible with future versions
#            line = dump_dotpar(item)
#            f.write(line+'\n')
                
def create_from_buffer(colname,buffer,rstrip=True,lstrip=True,cast=lambda val:val,comment=[]):
    """
    Create a collection from the buffer with just one column, containing the lines from buffer.
    Usually this function is useful for the line-by-line text processing.
    """
    items = []
    for line in buffer.split('\n'):
        if not line.strip(): continue
        if line.lstrip()[0] in comment: continue
        val = line
        if rstrip: val = val.rstrip()
        if lstrip: val = val.lstrip()
        val = cast(val)
        items.append({colname:val})
    col = Collection()
    col.update(items)
    return col
    
def create_from_buffer_multicol(buffer,cast={},duck=True,header=True,comment=[]):
    """
    Create a multicolumn collection from the buffer, using variable-length space delimiter.
    Colnames must obey the same rule as as an ordinary data line.
    Usually this function is useful for the line-by-line text processing.
    """
    
    # Skip first new lines.
    lines = buffer.split('\n')
    for istart,line in enumerate(lines):
        if line.strip(): break
            
    # Get column names.
    if header:
        names = lines[istart].split()        
        istart = istart+1
    else:
        names = ['c%d'%i for i in range(len(lines[0].split()))]        
    
    # Start parsing values.
    items = []    
    for line in lines[istart:]:
        if not line.strip(): continue
        if line.lstrip()[0] in comment: continue
        vals = line.split()  
        # Typing.
        item = {}
        for val,name in zip(vals,names):       
            if name in cast:  # cast typing
                val = cast[name](val)
            elif duck: # duck typing
                try:
                    val = int(val)
                except ValueError as e:
                    try:
                        val = float(val)
                    except ValueError as e:
                        pass
            else: # do nothing, treat as raw string
                pass
            item[name] = val
        items.append(item)
    
    # Create collection from the list of items.
    col = Collection()
    col.update(items)
    col.order = names
    
    return col
  
def join_index(idx1,idx2,inner=True):
    """ 
    
    ALTERNATIVE VERSION, WITH GROUP INDEXES INSTEAD OF COLLECTIONS
    
    Join two collections by key and additional conditions.
    
    idx1, idx2:
        group indexes of col1 and col2.
            
    Inner: 
        Flag to perform the inner join. 
        If False, outer join is performed.
        
    Output:
        Join index which is a list of ID pars (id1,id2) with
        id1 from col1 and id2 from col2.
    """    
                    
    def unroll(ids1,ids2): # unroll to list of pairs
        pairs = []
        for id1 in ids1:
            for id2 in ids2:
                pairs.append((id1,id2))
        return pairs
        
    # Calculate sets of indexvalues.
    keys1 = set(idx1.keys())
    keys2 = set(idx2.keys())
    
    # Prepare intersection for the inner part of join.
    keys_union = keys1.intersection(keys2)
    
    # Start forming the join index.
    idx = ft.reduce(lambda x,y:x+y,
        [unroll(idx1[k],idx2[k]) for k in keys_union])
    
    # If performing outer join, add symmetric difference set to it.
    if not inner:
        keys_diff_12 = keys1-keys2
        keys_diff_21 = keys2-keys1
        
        if keys_diff_12:
            idx += ft.reduce(lambda x,y:x+y,
                [unroll(idx1[k],[None]) for k in keys_diff_12])

        if keys_diff_21:
            idx += ft.reduce(lambda x,y:x+y,
                [unroll([None],idx2[k]) for k in keys_diff_21])
            
    return idx
  
def create_join_index(col1,col2,key,key2=None,inner=True):
    """ 
    Join two collections by key and additional conditions.
    
    col1,col2:
        collections to join together.
            
    key,key2:
        Key is a lambda function that must return comparable 
        object (e.g. tuple or scalar).  
    
    Inner: 
        Flag to perform the inner join. 
        If False, outer join is performed.
        
    Output:
        Join index which is a list of ID pars (id1,id2) with
        id1 from col1 and id2 from col2.
    """    
            
    def prepare_key(key): # convert key to lambda form
        if type(key) is str:
            key = eval('lambda v: v["%s"]'%key)
        elif type(key) in [tuple,list]:
            key = eval('lambda v: (%s)'%(','.join(['v["%s"]'%k for k in key])))
        elif type(key) is type(lambda:None):
            pass
        else:
            raise Exception('unknown key format: %s'%str(key))
        return key
        
    # Prepare keys and convert them to the lambda function format.
    key = prepare_key(key)
    if key2:
        key2 = prepare_key(key2)
    else:
        key2 = key
        
    # Calculate indexes for both collections.
    idx1 = col1.group(key)
    idx2 = col2.group(key2)
    
    idx = join_index(idx1,idx2,inner=inner)
    
    return idx

def join(col1,col2,idx,colnames1=lambda c:c,colnames2=lambda c:'_%s'%c):
    """
    Join two collection based on the result of the join_index/create_join_index functions.
    Colnames 1 and 2 are lambda functions renaming colnames, having the following format:
        colnames = lambda colname: <any name>
    """
    
    col = Collection()
    
    col.order = [colnames1(c) for c in col1.order] + \
                [colnames2(c) for c in col2.order]
    
    for id1,id2 in idx:
        item = {}
        if id1 is not None:
            item1 = col1.getitem(id1)
            for c in item1:
                c_ = colnames1(c)
                item[c_] = item1[c]
        if id2 is not None:
            item2 = col2.getitem(id2)
            for c in item2:
                c_ = colnames2(c)
                if c_ in item:
                    raise Exception('column conflict at join: %s'%c_)
                else:
                    item[c_] = item2[c]
        col.update(item)
    
    return col

def join_index_(*gidxs,inner=True):
    """ multi-collection version of join_index
    
    Join many collections by key and additional conditions.
    
    gidxs:
        group indexes (dicts)
            
    Inner: 
        Flag to perform the inner join. 
        If False, outer join is performed.
        
    Output:
        Join index which is a list of ID pars (id1,id2, ...idn) 
    """    
    
    def get_next(it): # get next value of an iterator
        return next(it,None)
        
    def get_ids(keys,gidxs): # get collection ids from a key tuple
        return [idx[key] if key is not None else [None] for key,idx in zip(keys,gidxs)]
    
    def cproduct(lists): # cartesian product of lists
        return it.product(*lists)
    
    def check_inner(flags):
        return all(flags)

    def check_outer(flags):
        return any(flags)
    
    def iterate(iters,check): # keys must not be None!!!
        curkeys = [get_next(it) for it in iters]
        keyss = []
        while True:
            #print('curkeys>>>',curkeys)
            if all([k is None for k in curkeys]):
                break
            minkey = min([k for k in curkeys if k is not None])
            flags = [k==minkey for k in curkeys]
            if check(flags):
                keys = [minkey if f else None for f in flags]
                keyss.append([minkey,keys])
            curkeys = [get_next(it) if f else k for k,f,it in zip(curkeys,flags,iters)]
        #print('keyss>>>',keyss)
        return keyss
    
    iterate_inner = lambda key_iterators: iterate(key_iterators,check=check_inner)
    iterate_outer = lambda key_iterators: iterate(key_iterators,check=check_outer)
            
    key_iterators = [iter(sorted(idx.keys())) for idx in gidxs]
    
    if inner:
        meta_iterator = iterate_inner(key_iterators)
    else:
        meta_iterator = iterate_outer(key_iterators)
    
    jidx = []
    for val,keys in meta_iterator:
        ids = get_ids(keys,gidxs)
        jidx_ = cproduct(ids)
        jidx += [[val,i] for i in jidx_]
            
    return jidx

#def create_join_index_(cols,keys,inner=True):
#    """ multi-collection version of create_join_index """
#    idxs = [col.group(key) for col,key in zip(cols,keys)]
#    idx = join_index_(idxs,inner=inner)
#    return idx

def join_(jkey,jidx,*cols):
    """ 
    Multi-collection version of join.
    Inputs: 
        jkey -> name of join key (i.e. where to store keyvals of jidx)
                can also be a list/tuple of names with the same length
                as keys of jidx.
        jidx -> join index produced by join_index_()
        cols -> columns to join.
    To reduce/rename column keys now one must use
    slice and map collection methods respectively.
    """

    col_join = Collection()
    
    col_join.order = ft.reduce(lambda x,y: x+y,[c.order for c in cols])
    
    if type(jkey) is str:
        init_item = lambda jval: {jkey:jval}
        col_join.order = [jkey] + col_join.order
    elif type(jkey) in {tuple,list}:
        init_item = lambda jval: {k:v for k,v in zip(jkey,jval)}
        col_join.order = list(jkey) + col_join.order
    else:
        raise Exception('jkey must be either string, list or tuple')
            
    for jval,keys in jidx:        
        item = init_item(jval)
        for ID,col in zip(keys,cols):
            if ID is None:
                continue
            item_ = col.getitem(ID)
            for k in item_:
                if k in item:
                    raise Exception('column conflict at join: %s'%k)
                else:
                    item[k] = item_[k]
        col_join.update(item)
    
    return col_join
    
######################################################################
# RECURSIVE COMPARISON OF THE ARBITRARY DATA STRUCTURES, "DIFF". #####
# SHOULD CORRECTLY COMPARE NESTED STRUCTURES OF DICTS ################
######################################################################

def diff(D1,D2):
    """
    Recursvely compare two nested structures consisting of dict
    objects. These objects should contain elements which are comparable,
    i.e. supporting the "==" operation.
    The result of the diff is ??????
    """
    raise NotImplementedError
    # get keys of data structures
    keys1 = data1.keys()
    keys2 = data2.keys()  
      
    # get three different areas of comparison:
    #  a) D1 minus D2 
    #  b) D2 minus D1
    #  c) D1 intersect D2
    
    output = {}
    
    D1_minus_D2 = set(D1)-set(D2) # always part of output
    if D1_minus_D2: output['left'] = {D1[key] for key in D1_minus_D2}
    
    D2_minus_D1 = set(D2)-set(D1) # always part of output
    if D2_minus_D1: output['right'] = {D2[key] for key in D2_minus_D1}
    
    D1_intersect_D2 = set(D1).intersect(D2) # part of output if there are 
    center = {}
    for key in D1_intersect_D2:
        e1 = D1[key]
        e2 = D2[key]
        if type(e1)==dict and type(e2)==dict:
            out = diff(e1,e2)
            if out: center[key] = out
        elif type(e1)==dict and type(e2)!=dict:
            out = {}
