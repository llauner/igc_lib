import _pickle as cPickle
import hashlib

class HashHelper(object):
    """description of class"""

    @classmethod
    def ComputeHashForList(cls, list):
        '''
        Get a Hash representing the list<string>
       
        Keyword arguments:
        list -- the list to be hashed
        '''

        if (list is None or not list):
            return None

        sorted_list = sorted(list)
        p = cPickle.dumps(sorted_list, -1)

        return hashlib.md5(p).hexdigest()
