from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "abcd", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    abcd__table_0 = Process(
        name = "abcd__Table_0",
        properties = ModelTransform(modelName = "abcd__Table_0"),
        input_ports = None
    )

