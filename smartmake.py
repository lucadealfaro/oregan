import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import re
import subprocess
import traceback
import yaml

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
    
    def __init__(self, name=None, dependencies=None, command=None, generates=None):
        """
        :param root_path: root path where to create files. 
        :param name: name of task
        :param dependencies: FileSpec dependencies of task (predecessors in the graph).
        :param command: command (with parameters inside) to build the target.
        :param generates: list of FileSpec that are built. 
        """
        self.name = name
        self.dependencies = dependencies or []
        self.command = command or None
        self.generates = generates or []
        self.params = get_params(command)
        for g in self.generates:
            self.params.update(get_params(g))
        
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
                    generates=[g.concretize(root_path, params) for g in self.generates],
                    redo_if_modified=redo_if_modified
                    )
              
        
class Task(object):
    """This is a concrete task, where the parameters are resolved."""
    
    def __init__(self, name=None, dependencies=None, command=None, generates=None,
                 redo_if_modified=False):
        """
        :param name: name of task
        :param dependencies: File dependencies of task (predecessors in the graph).
        :param command: command (with parameters inside) to build the target.
        :param generates: list of File that are built. 
        :param redo_if_modified: if True, use modification times rather than 
            existence to decide whether to run. 
        """
        self.name = name
        self.file_dependencies = dependencies or []
        self.command = command or None
        self.generates = generates or []
        self.done = False # In preparation for the execution. 
        # Dependencies in terms of tasks. 
        self.task_dependencies = []
        self.futures_dependencies = []
        # Successors in order of execution
        self.task_successors = []
        # Its own future.
        self.future = None
        
    def needs_running(self):
        """
        Returns True/False according to whether we need to run the task. 
        NOTE: This MUST be called in topological order, dependencies first, 
        otherwise the propagation of modification times will not work properly.
        """
        for g in self.generates:
            g.refresh()
        if any(not g.exists for g in self.generates):
            return True
        if self.redo_if_modified:
            for g in self.generates:
                for d in self.file_dependencies:
                    d.refresh()
                    if d.time > g.time:
                        return True
        return False
    
    def run(self):
        """This is called by the task executor to cause this concrete task to run."""
        if self.needs_running():
            # First, waits for all predecessors to have finished.
            for f in self.futures_dependencies:
                if not f.result():
                    # The job failed. 
                    return False
            # Runs the task.
            print("Running", self.command)
            try:
                subprocess.run(self.command.split())
            except Exception as e:
                traceback.print_exc(e)
                return False 
            print("Done", self.command)
        else:
            print("Task", self.command, "does not need re-running.")
        # Done. 
        return True
    
        
class MakeGraph(object):
    """Abstract (parameterized) graph of SmartMake dependencies."""
    
    def __init__(self, root_path):
        self.root_path = root_path
        # List of tasks. 
        self.tasks = []
        # Mapping from file name, to the task that generates that file. 
        self.rules = {}
        
    def add_task(self, task):
        self.tasks.append(task)
        for g in task.generates:
            self.rules[g.name] = task
        
    def concretize(self, target_name, params, graph=None, redo_if_modified=False):
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
        to_add = {target_name} # We keep track of all the target names we need to build.
        done = set()
        file_to_task = {} # Mapping from each file to the task that generates it. 
        while len(to_add) > 0:
            name = to_add.pop()
            done.add(name)
            # Adds the concrete task.
            task = self.rules[name]
            concrete_task = task.concretize(self.root_path, params, redo_if_modified=redo_if_modified)
            for g in concrete_task.generates:
                file_to_task[g] = concrete_task
            g.tasks.append(concrete_task)
            # Adds the dependencies to what should be added.
            to_add |= {d.name for d in task.dependencies} - done
        # Now wires the dependencies and successors in the concrete graph.
        for concrete_task in g.tasks:
            for d in concrete_task.file_dependencies:
                predecessor_task = file_to_task[d]
                predecessor_task.task_successors.append(concrete_task)
                concrete_task.task_dependencies.append(predecessor_task)
    
    
class CommandGraph(object):
    """Concrete graph, whose nodes represent files that have commands to make them."""

    def __init__(self):
        self.tasks = []
                    
    def run(self, parallelism=1):
        """Runs the current graph. 
        :param parallelism: how many tasks to run in parallel. 
        """
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            # Puts all the concrete tasks in the executor, in topological order. 
            # Before a task is added, we fix the set of all the futures on which 
            # it needs to wait. 
            to_add = {t for t in self.tasks if len(t.task_dependencies) == 0}
            while len(to_add) > 0:
                t = to_add.pop()
                # Fixes the futures dependencies of t. 
                for pre_t in t.task_dependencies:
                    t.futures_dependencies.add(pre_t.future)
                # And adds it to the thread pool executor. 
                t.future = executor.submit(t.run)
                # If a successor of t has all the predecessors already submitted, 
                # we can schedule it to be added.
                for succ_t in t.task_successors:
                    if succ_t.future is None and all(tt.future is not None for tt in succ_t.task_dependencies):
                        to_add.add(succ_t)
            # Ok, now tasks are all in the thread pool.  We just need to wait for all of them to finish.
            return all(t.result() for t in self.tasks)
    
    
def add_tasks(param_names, args, params, g, cg, target):
    """Adds to the concrete graph cg all the things to do due to the given 
    target, for all combination of parameters."""
    if len(param_names) > 0:
        p_name = param_names[0]
        p_value = getattr(args, p_name)
        if p_value is not None:
            # A value has been specified.  It can be a single value, or a list of values.
            p_values = p_value.split(",")
            for v in p_values:
                params[p_name] = v
                add_tasks(param_names[1:], args, params, g, cg, target)
        # No value specified, skips parameter.
        add_tasks(param_names[1:], args, params, g, cg, target)
    else:
        # We have the values of all parameters, we concretize the graph.
        g.concretize(target, params, graph=cg, redo_if_modified=args.redo_if_modified)                
        
        
def main(parser, definitions):
    # First, parses the argument values.    
    for p_name, p_help in definitions["parameters"].items():
        parser.add_argument("--" + p_name.key(), type=str, default=None, help=p_help)
    args = parser.parse_args()
    # Builds the files. 
    names_to_filespec = {name: FileSpec(name, path) 
                         for name, path in definitions["files"].items()}
    # Then, builds the abstract graph. 
    g = MakeGraph(args.root_path)
    for t_desc in definitions["tasks"]:
        t = TaskSpec(
            name=t_desc["name"], 
            command=t_desc["command"],
            target=names_to_filespec[t_desc["generates"]],
            dependencies=[names_to_filespec[d] for d in t_desc.get("dependencies", [])]
            )
        g.add_task(t)
    # Builds a single concrete graph.
    cg = CommandGraph()
    # Now adds to the command graph the concretizations of all the things to do.
    add_tasks(list(definitions["parameters"].keys()), args, {}, g, cg, args.target)
    # The concrete graph at this point contains all concrete tasks, and we can run it.
    cg.run(parallelism=args.parallelism)
    # That's all, folks. 
    print("All done.")


parser = argparse.ArgumentParser()
parser.add_argument("yaml_file", type=str, help="Yaml file describing the dependencies.")
parser.add_argument("--root_path", type=str, help="Root path for the files.")
parser.add_argument("--target", type=str, help="Target file to be built (use the name in the yaml file)")
parser.add_argument("--redo_if_modified", default=False, action="store_true",
                    help="If set, recomputer files that have a prior modification date than their dependencies.")
parser.add_argument("--parallelism", type=int, default=1,
                    help="Number of parallel processes used during the build.")
args = parser.parse_args()
with open(args.yaml_file) as f:
    definitions = yaml.load(f, yaml.SafeLoader)
main(parser, definitions)