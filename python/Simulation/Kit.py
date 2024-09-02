class Kit:
    def __init__(self, id, code, ns, count = 1, isContainedKit = False, numOfKitsContained = None):
        self._id = id
        self._code = code
        self._ns = ns
        self._count = count
        self._isContainedKit = isContainedKit
        self._numOfKitsContained = numOfKitsContained

    def __dict__(self):
        return {
            "id": self._id,
            "code": self._code,
            "ns": self._ns,
            "count": self._count,
            "isContainedKit": self._isContainedKit,
            "numOfKitsContained": self._numOfKitsContained
        }
    
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, value):
        self._code = value

    @property
    def ns(self):
        return self._ns

    @ns.setter
    def ns(self, value):
        self._ns = value

    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, value):
        self._count = value

    @property
    def isContainedKit(self):
        return self._isContainedKit

    @isContainedKit.setter
    def isContainedKit(self, value):
        self._isContainedKit = value

    @property
    def numOfKitsContained(self):
        return self._numOfKitsContained

    @numOfKitsContained.setter
    def numOfKitsContained(self, value):
        self._numOfKitsContained = value

