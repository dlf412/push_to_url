import simplejson as json

'''
translate json format file into config object
'''
class Mwconfig(object):

    @classmethod
    def _format(cls, obj):
        if isinstance(obj, dict):
            return cls(obj)
        if isinstance(obj, list):
            return [cls._format(e) for e in obj]
        else:
            return obj

    def __init__(self, obj):
        '''
        support filename and dict
        '''
        if isinstance(obj, basestring):
            with open(obj) as fp:
                super(self.__class__, self).__setattr__('_obj', json.load(fp))
        elif isinstance(obj, dict):
            super(self.__class__, self).__setattr__('_obj', obj)
        else:
            raise Exception("invalid params for construction")

        for key, value in self._obj.items():
            self._obj[key] = self._format(value)

    def __repr__(self):
        return str(self._obj)

    def __setattr__(self, attr, value):
        self._obj[attr] = value

    def __getattr__(self, attr):
        return self._obj[attr]

    def __contains__(self, attr):
        return hasattr(self, attr)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def items(self):
        for x, y in self._obj.iteritems():
            yield x, y

    def __len__(self):
        return len(self._obj)

    def __iter__(self):
        for x in self._obj:
            yield x

    def __next__(self):
        pass

    def get(self, attr, default):
        try:
            return getattr(self, attr)
        except KeyError:
            setattr(self, attr, default)
            return default

if __name__ == '__main__':
    etc_file = '../etc/config.json'
    mycfg = Mwconfig(etc_file)

    def show(cfg):
        if isinstance(cfg, Mwconfig):
            for k, v in cfg.items():
                print k, v
                if isinstance(v, Mwconfig):
                    show(v)
                if isinstance(v, list):
                    for o in v:
                        show(o)
        elif isinstance(cfg, list):
            for o in cfg:
                show(o)
        elif isinstance(cfg, basestring):
            print cfg

    show(mycfg)

    print len(mycfg)
    for strategy in mycfg.strategy.long.task:
        print strategy.duration, strategy.times

    exit(0)

