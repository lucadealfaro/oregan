import argparse
import threading
import os
import re
import subprocess
import sys
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
    
    def __init__(self, path):
        self.path = path
        self.exists = os.path.exists(path)
        self.time = os.path.getmtime(path) if self.exists else None
        
    def refresh(self):
        self.exists = os.path.exists(self.path)
        self.time = os.path.getmtime(self.path) if self.exists else None
        print("File", self.path, "exists", self.exists)
        

class TaskSpec(object):
    """This class represents a node of the parameterized graph."""
    
    def __init__(self, name=None, dependencies=None, command=None, generates=None, uses=None):
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
        self.uses = uses or []
        for g in self.generates:
            self.params.update(get_params(g.path))
        for d in self.dependencies:
            self.params.update(get_params(d.path))
        
    def concretize(self, root_path, params, redo_if_modified=False):
        """Generates the concrete task.
        :param root_path: root path in the filesystem.
        :param params: parameters for concretization.
        :param redo_if_modified: if True, use modification times rather than 
            existence to decide whether to run. 
        """
        param_set = set(params.keys())
        if not param_set >= self.params:
            raise MissingParameters(self.name, self.params - param_set)
        return Task(name=self.name + repr(params), 
                    dependencies=[d.concretize(root_path, params) for d in self.dependencies],
                    uses=self.uses,
                    command=self.command.format(**params),
                    generates=[g.concretize(root_path, params) for g in self.generates],
                    redo_if_modified=redo_if_modified
                    )
              
        
class Task(object):
    """This is a concrete task, where the parameters are resolved."""
    
    def __init__(self, name=None, dependencies=None, command=None, generates=None,
                 redo_if_modified=False, uses=None):
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
        self.redo_if_modified = redo_if_modified
        self.generates = generates or []
        # Resouces it uses. 
        self.uses = uses or []        
        # Condition variable for this task. 
        self.done = threading.Condition()
        self.completed = False
        # Dependencies in terms of tasks. 
        self.task_dependencies = [] # These are tasks. 
        # Successors in order of execution
        self.task_successors = []
        # Did the task succeed? 
        self.success = False
        
    def __repr__(self):
        return "Name: {} Command: {}".format(self.name, self.command)
        
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
    
    def run(self, thread_semaphore):
        """This is called by the task executor to cause this concrete task to run."""
        if self.needs_running():
            # First, waits for all predecessors to have finished.
            for t in self.task_dependencies:
                if not t.completed:
                    with t.done:
                        t.done.wait() 
                if not t.success:
                    print("Job {} cannot be done as some dependency failed".format(self))
                    # The job failed. 
                    self.success = False
                    self.completed = True
                    with self.done:
                        self.done.notify_all()
                    return
            # We acquire the resources. 
            [r.acquire() for r in self.uses]
            # We acquire the threads. 
            thread_semaphore.acquire()
            # Runs the task.
            print("Running", self.command)
            try:
                subprocess.run(self.command, shell=True, check=True)
                self.success = True
                print("Done:", self.command)
            except Exception as e:
                print("Failed command:", self.command)
                self.success = False
            finally:
                # Release the thread.
                thread_semaphore.release()
                # Releases any resources, in the opposite order in which they were acquired.
                [r.release() for r in reversed(self.uses)]
                # We are done. 
                self.completed = True
                with self.done:
                    self.done.notify_all()
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
        # Mapping from file definition name, to the (abstract) task that generates that file. 
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
        generated_tasks = []
        while len(to_add) > 0:
            name = to_add.pop()
            done.add(name)
            # Adds the concrete task.
            task = self.rules[name]
            concrete_task = task.concretize(self.root_path, params, redo_if_modified=redo_if_modified)
            print("params:", params, "Concrete task path:", [d.path for d in concrete_task.file_dependencies])
            for generated in concrete_task.generates:
                g.path_to_task[generated.path] = concrete_task
            g.tasks.append(concrete_task)
            generated_tasks.append(concrete_task)
            # Adds the dependencies to what should be added.
            to_add |= {d.name for d in task.dependencies} - done
        # Now wires the dependencies and successors in the concrete graph.
        for concrete_task in generated_tasks:
            for d in concrete_task.file_dependencies:
                predecessor_task = g.path_to_task[d.path]
                predecessor_task.task_successors.append(concrete_task)
                concrete_task.task_dependencies.append(predecessor_task)
    
    
class CommandGraph(object):
    """Concrete graph, whose nodes represent files that have commands to make them."""

    def __init__(self):
        self.tasks = []
        self.path_to_task = {}
        
    def __repr__(self):
        return "Tasks: \n" + "\n".join([repr(t) for t in self.tasks])
                    
    def run(self, parallelism=1):
        """Runs the current graph. 
        :param parallelism: how many tasks to run in parallel. 
        """
        # Creates the thread semaphore.
        thread_semaphore = threading.BoundedSemaphore(value=parallelism)
        # We just run everything, leaving the coordination to the condition 
        # variables and the thread semaphore.
        my_threads = []
        for t in self.tasks:
            th = threading.Thread(target=t.run, args=[thread_semaphore])
            my_threads.append(th)
            th.start()
        # Waits for all the work to be done.
        for th in my_threads:
            th.join()
        return all(t.success for t in self.tasks)
    
    
def add_tasks(param_names, args, params, g, cg, target):
    """Adds to the concrete graph cg all the things to do due to the given 
    target, for all combination of parameters."""
    if len(param_names) > 0:
        p_name = param_names[0]
        p_value_list = getattr(args, p_name)
        if len(p_value_list) > 0:
            for v in p_value_list:
                params[p_name] = v
                add_tasks(param_names[1:], args, params, g, cg, target)
        else:
            # No value specified, skips parameter.
            add_tasks(param_names[1:], args, params, g, cg, target)
    else:
        # We have the values of all parameters, we concretize the graph.
        g.concretize(target, params, graph=cg, redo_if_modified=args.redo_if_modified)                
        
        
def main(definitions):
    # First, parses the argument values.    
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_file", type=str, help="Yaml file describing the dependencies.")
    parser.add_argument("--root_path", type=str, help="Root path for the files.")
    parser.add_argument("--target", type=str, help="Target file to be built (use the name in the yaml file)")
    parser.add_argument("--redo_if_modified", default=False, action="store_true",
                        help="If set, recomputer files that have a prior modification date than their dependencies.")
    parser.add_argument("--parallelism", type=int, default=1,
                        help="Number of parallel processes used during the build.")
    for p_name, p_help in definitions["parameters"].items():
        parser.add_argument("--" + p_name, type=str, nargs="*", default=[], help=p_help)
    args = parser.parse_args()
    # Builds the files. 
    names_to_filespec = {name: FileSpec(name, path) 
                         for name, path in (definitions.get("files") or {}).items()}
    # Builds the resources.
    name_to_resource = {name: threading.BoundedSemaphore(value=int(v))
                        for name, v in (definitions.get("resources") or {}).items()}
    # Then, builds the abstract graph. 
    g = MakeGraph(args.root_path)
    for t_desc in definitions["tasks"]:
        t = TaskSpec(
            name=t_desc["name"], 
            command=t_desc["command"],
            generates=[names_to_filespec[g] for g in (t_desc.get("generates", []) or [])],
            dependencies=[names_to_filespec[d] for d in (t_desc.get("dependencies", []) or [])],
            uses=[name_to_resource[n] for n in (t_desc.get("uses", []) or [])]
            )
        g.add_task(t)
    # Builds a single concrete graph.
    cg = CommandGraph()
    # Now adds to the command graph the concretizations of all the things to do.
    add_tasks(list(definitions["parameters"].keys()), args, {}, g, cg, args.target)
    print(cg)
    # The concrete graph at this point contains all concrete tasks, and we can run it.
    cg.run(parallelism=args.parallelism)
    # That's all, folks. 
    print("All done.")


yaml_file = sys.argv[1]
with open(yaml_file) as f:
    definitions = yaml.load(f, yaml.SafeLoader)
main(definitions)