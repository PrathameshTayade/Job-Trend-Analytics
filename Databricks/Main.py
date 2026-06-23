import sys
import importlib
import traceback
from pyspark.sql import SparkSession
from run_context import Run_context
spark = SparkSession.builder.getOrCreate()

job_param = sys.argv[1]
slot = sys.argv[2]
submission = sys.argv[3]
root = sys.argv[4]
dataprovider = sys.argv[5]
runid = sys.argv[6]
layer = sys.argv[7]

run  = Run_context(
    slot = slot,
    submission = submission,
    root = root,
    dataprovider = dataprovider,
    runid = runid
    
)

try : 
    module_name,function_name = job_param.split(".")
    module = importlib.import_module(f"Jobs.{layer}.{module_name}")
    func = getattr(module,function_name)
    print("Exection started")


    func(run)
    print("Exection Completed")
except Exception as e:
    print(f">>> Error Executing {job_param}")
    print(str(e))
    traceback.print_exc()
    raise
