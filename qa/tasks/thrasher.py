"""
Thrasher base class
"""
class Thrasher:

    def __init__(self):
        print "init start"
        self.exception = None
        print "init end"

    def setexception(self, e):
        """
        Sets the exception.
        Called by the subclasses to store the exception.
        """
        self.exception = e

    def getexception(self):
        """
        returns the exception
        """
        return self.exception
