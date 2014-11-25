import traceback

class FlowElement(object):
    def __init__(self,connector=None,method=None,connector_largs=[],connector_kwargs={}):
        self.connector=connector
        self.connector_kwargs=connector_kwargs
        self.connector_largs=connector_largs
        self.method=method
        if type(self)!=NullProcessor:
            self.out_objects=[NullProcessor()]
            self.out_in_objects=[NullProcessor()]
            self.error_objects=[NullProcessor()]
    def compute_args(self,elem={}):
        new_largs=[x for x in self.connector_largs]
        new_kwargs=self.connector_kwargs.copy()
        for i,e in enumerate(new_largs):
            if type(e)==Ev:
                new_largs[i]=e.evaluate(elem)
        for k,v in new_kwargs.iteritems():
            if type(v)==Ev:
                new_kwargs[k]=v.evaluate(elem)
        return new_largs,new_kwargs
    def out(self,*out_objects):
        self.out_objects=out_objects
        return self
    def error(self,*error_objects):
        self.error_objects=error_objects
        return self
    def out_in(self,*out_in_objects):
        self.out_in_objects=out_in_objects
        return self

    def out_elem(self,e):
        for o in self.out_objects:
            o.process(e)
    def out_in_elem(self,e):
        for o in self.out_in_objects:
            o.process(e)
    def error_elem(self,e):
        for o in self.error_objects:
            o.process(e)

class NullProcessor(FlowElement):
    def process(self,elem={}):
        return

class ListProcessor(FlowElement):
    def process(self,elem={}):
        largs,kwargs=self.compute_args(elem)
        for x in getattr(self.connector,self.method)(*largs,**kwargs):
            self.out_elem(x)

class ElementProcessor(FlowElement):
    def process(self,elem={}):
        largs,kwargs=self.compute_args(elem)
        try:
            self.out_elem(getattr(self.connector,self.method)(*largs,**kwargs))
            self.out_in_elem(elem)
        except:
            error=traceback.format_exc()
            elem['error']=error
            self.error_elem(elem)



class ElementFilter(FlowElement):
    def __init__(self,filter=None):
        self.filter=filter
        self.out_objects=[NullProcessor()]
        self.out_in_objects=[NullProcessor()]
        self.error_objects=[NullProcessor()]
    def process(self,elem={}):
        if not self.filter or self.filter.evaluate(elem):
            self.out_elem(elem)

class ListAcumulator(FlowElement):
    def __init__(self):
        self.list=[]
        self.out_objects=[NullProcessor()]
        self.out_in_objects=[NullProcessor()]
        self.error_objects=[NullProcessor()]
    def process(self,elem={}):
        self.list.append(elem)
        self.out_elem(elem)

    def count(self):
        return len(self.list)

class BreakPoint(FlowElement):
    def __init__(self,**debug_vars):
        self.list=[]
        self.out_objects=[NullProcessor()]
        self.error_objects=[NullProcessor()]
        self.out_in_objects=[NullProcessor()]
        self.debug_vars=debug_vars
    def process(self,elem={}):
        import pdb
        pdb.set_trace()
        self.out_elem(elem)
        self.out_in_elem(elem)


class Printer(FlowElement):
    def __init__(self,prefix=''):
        self.prefix=prefix
        self.out_objects=[NullProcessor()]
        self.out_in_objects=[NullProcessor()]
        self.error_objects=[NullProcessor()]
    def process(self,elem):
        print self.prefix,elem
        self.out_elem(elem)

class ElementTransformer(FlowElement):
    def __init__(self):
        self.out_object=[NullProcessor()]
        self.out_in_objects=[NullProcessor()]
        self.error_objects=[NullProcessor()]
    def process(self,elem):
        self.out_elem(self.transform(elem))
    def transform(self,elem):
        return elem

class Ev(object):
    def __init__(self,expression):
        self.expression=expression

    def evaluate(self,elem):
        return eval(self.expression)


