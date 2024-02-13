# Oregan

License: BSD

A traditional Makefile encodes dependencies between files.

When doing experiments, there are often experiment parameters (e.g., the num_simulations that are performed).
In general, in a scientific experiment, there is data dependency: the whole dependency chain needs to be run with the given parameters. 
Oregan tracks parameter dependencies in data, and schedules the code to be run to generate the data with the correct parameters. 

It is called via: 

    python oregan.py <spec.yaml> --root_path=<file_root_path> --target=<target_name> [parameters]

where: 

* spec.yaml is a yaml file containing the make specifications.  It contains parameter, file, and task specifications. 
* `root_path` is the root used for checking the file existence during the make process. 
* `target` is the target file (as defined in the YAML file) that we wish to build. 
* After this come the parameter specifications. 

As an example, the code can be tested with: 

    python oregan.py tests/test.yaml --root_path=tests --target=h_abc --a=1 --b=2 --c=3

There are two additional parameters: 

`--redo_if_modified` : if specified, files are rebuilt when they have a prior modification date than some of their dependencies.  This is the standard make behavior.  By default, instead, files are rebuilt only when they are missing. 

`--parallelism` is the maximum number of commands executing in parallel. 

Author: Luca de Alfaro, 2023.

## Example

Assume you have three tasks, `GenerateF`, `GenerateG`, `Together`.  
Each of `GenerateF`, `GenerateG` generates some files, that `Together` then puts together. 

## TODO

* Enable reading the parameters from a file via a --param file.yaml option. 
* Define exclusive resources (GPUs) for scheduling. 
