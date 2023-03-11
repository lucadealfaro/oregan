import os
import re
# Regexp for finding all parameters.
param_regex = re.compile(r"\{([^\}]*)\}")

def get_params(s):
    """Returns the list of parameters in the string s."""
    return set(re.findall(param_regex, s))


class MissingParameters(Exception):

    def __init__(self, place, params):
        super().__init__("In {}, missing parameters: {}".format(
            place, " ".join(params)
        ))
        

class FileSpec(object):
    """Class for storing a file specification."""
    
    def __init__(self, fileroot, path):
        self.fileroot = fileroot
        self.path = path
        self.params = get_params(path)
        
    def instantiate(self, params):
        """Returns the actual file."""
        if not self.params <= params:
            raise MissingParameters(self.path, self.params - params)
        return File(os.path.join(self.fileroot, self.path.format(**params)))
    
    
class File(object):
    """Class for storing a file, with its parameters instantiated."""
    
    def __init__(self, abspath):
        self.abspath = abspath
        self.exists = os.path.exists(abspath)
        self.date = os.path.getmtime(abspath) if self.exists else None
        

class TaskSpec(object):
    
    def __init__(self, fileroot, name=None, command=None, 
                 dependencies=None, generates=None):
        self.fileroot = fileroot
        self.name = name
        self.command = command
        self.dependencies = dependencies
        self.generates = generates
        # The parameters are parameters of the command or of the target.
        self.params = get_params(command) | generates.params
        
        

        