import os
import sys

testing_directory: str = os.path.dirname(__file__)
module_directory: str = r"../src"
sys.path.insert(0, os.path.abspath(os.path.join(testing_directory, module_directory)))
