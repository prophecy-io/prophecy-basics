from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "abcd", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    abcd__testtrainsplit_1 = Process(
        name = "abcd__TestTrainSplit_1",
        properties = ModelTransform(modelName = "abcd__TestTrainSplit_1"),
        output_ports = 2
    )

