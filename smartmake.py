from concurrent.futures import ThreadPoolExecutor
import os
import re
import subprocess

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
    
    def __init__(self, name, path):
        """Creates a file specification.
        :param name: name (nickname) of file.
        :param path: path.  Can contain parameters.  
        """
        self.name = name
        self.path = path
        self.params = get_params(path)
        
    def concretize(self, fileroot, params):
        """Returns the actual file."""
        return File(os.path.join(fileroot, self.path.format(**params)))
        
    
class File(object):
    """Class for storing a file, with its parameters instantiated."""
    
    def __init__(self, name, path):
        self.path = path
        self.exists = os.path.exists(path)
        self.time = os.path.getmtime(path) if self.exists else None
        
    def refresh(self):
        self.exists = os.path.exists(self.path)
        self.time = os.path.getmtime(self.path) if self.exists else None
        

class TaskSpec(object):
    """This class represents a node of the parameterized graph."""
    
    def __init__(self, name=None, dependencies=None, command=None, target=None):
        """
        :param root_path: root path where to create files. 
        :param name: name of task
        :param dependencies: FileSpec dependencies of task (predecessors in the graph).
        :param command: command (with parameters inside) to build the target.
        :param target: FileSpec that is built. 
        """
        self.name = name
        self.dependencies = dependencies or []
        self.command = command or None
        self.target = target
        self.params = get_params(target) | get_params(command)
        
    def concretize(self, root_path, params, redo_if_modified=False):
        """Generates the concrete task.
        :param root_path: root path in the filesystem.
        :param params: parameters for concretization.
        :param redo_if_modified: if True, use modification times rather than 
            existence to decide whether to run. 
        """
        if not params >= self.params:
            raise MissingParameters(self.name, self.params - params)
        return Task(name=self.name, 
                    dependencies=[d.concretize(root_path, params) for d in self.dependencies],
                    command=self.command.format(**params),
                    target=self.target.concretize(root_path, params),
                    redo_if_modified=redo_if_modified
                    )
              
        
class Task(object):
    """This is a concrete task, where the parameters are resolved."""
    
    def __init__(self, name=None, dependencies=None, command=None, target=None,
                 redo_if_modified=False):
        """
        :param name: name of task
        :param dependencies: File dependencies of task (predecessors in the graph).
        :param command: command (with parameters inside) to build the target.
        :param target: File that is built. 
        :param redo_if_modified: if True, use modification times rather than 
            existence to decide whether to run. 
        """
        self.name = name
        self.dependencies = dependencies or []
        self.command = command or None
        self.target = target
        self.done = False # In preparation for the execution. 
        
    def needs_running(self):
        """
        Returns True/False according to whether we need to run the task. 
        NOTE: This MUST be called in topological order, dependencies first, 
        otherwise the propagation of modification times will not work properly.
        """
        self.target.refresh()
        if not self.target.exists:
            return True
        if self.redo_if_modified:
            target_time = self.target.time
            for d in self.dependencies:
                d.refresh()
                if d.time > self.target.time:
                    return True
        return False
    
    def run(self):
        """This is called by the task executor to cause this concrete task to run."""
        if self.needs_running():
            # Runs the task.
            print("Running", self.command)
            subprocess.run(self.command.split())
            print("Done", self.command)
        else:
            print("Task", self.command, "does not need re-running.")
        # Done. 
        self.done = True
        return self            
    
        
class MakeGraph(object):
    """Abstract (parameterized) graph of SmartMake dependencies."""
    
    def __init__(self, root_path):
        self.root_path = root_path
        # List of tasks. 
        self.tasks = []
        # Mapping from file name, to the task that creates that file. 
        self.rules = {}
        
    def add_task(self, task):
        self.tasks.append(task)
        self.rules[task.target.name] = task
        
    def generate(self, target_name, params, graph=None, redo_if_modified=False):
        """Generates a concrete graph for building the given target name
        with the given parameters.
        :param target_name: name of the target for which we have to add the task. 
        :param params: parameters used for the concretization. 
        :param graph: A CommandGraph. If specified, nodes are added to this graph, so we can build a global 
            list of tasks to be done. 
        :param redo_if_modified: if True, use modification times rather than 
            existence to decide whether to run. 
        """
        g = CommandGraph() if graph is None else graph
        to_add = {target_name}
        done = set()
        while len(to_add) > 0:
            name = to_add.pop()
            done.add(name)
            # Adds the concrete task.
            task = self.rules[name]
            g.add_task(task.concretize(self.root_path, params, redo_if_modified=redo_if_modified))
            # Adds the dependencies to what should be added.
            to_add |= {d.name for d in task.dependencies} - done
            
    
class CommandGraph(object):
    """Concrete graph, whose nodes represent files that have commands to make them."""

    def __init__(self, parallelism=1):
        self.tasks = []
        # No two tasks can refer to the same target. 
        self.targets = set()
        
    def add_task(self, task):
        if task.target.path not in self.targets:
            self.tasks.append(task)
            self.targets.add(task.target.path)
            
    def run(self, parallelism=1):
        """Runs the current graph. 
        :param parallelism: how many tasks to run in parallel. 
        """
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            def task_done(f):
                """This function is called when a task is done"""
            # Finds the initial tasks that can be done. 
            initial_tasks = [t for t in self.tasks if len(t.dependencies) == 0]
            for t in initial_tasks:
                executor.submit(t.run).add_done_callback(task_done)
        
            
    
        
def create(target_filespec, params, redo_if_modified=False):
    """Asks to create the target filespec with the given set
    of paramters.
    :param target_filespec: filespec to be created. 
    :param params: paramters of filespec to be created. 
    :param redo_if_modified: If True, recreates the resource if
        any source has a more recent modification date.  This is
        what the classical Make does.   
    """
    
        
        

        