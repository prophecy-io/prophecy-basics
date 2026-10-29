from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "pip", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    customer_csv_1 = Process(
        name = "customer_csv_1",
        properties = SharepointSource(
          format = SharepointSource.CsvReadFormat(schema = "external_sources/pip/customer_csv_1.yml"),
          compression = SharepointSource.Compression(kind = "uncompressed"),
          properties = SharepointSource.SharepointSourceInternal(filePath = "/customer.csv"),
          connector = "sharepoint_1"
        ),
        input_ports = None
    )
    pip__testtrainsplit_0_out0 = Process(
        name = "pip__TestTrainSplit_0_out0",
        properties = ModelTransform(modelName = "pip__TestTrainSplit_0_out0")
    )
    pip__testtrainsplit_0_out1 = Process(
        name = "pip__TestTrainSplit_0_out1",
        properties = ModelTransform(modelName = "pip__TestTrainSplit_0_out1")
    )
    pip__testtrainsplit_0_out2 = Process(
        name = "pip__TestTrainSplit_0_out2",
        properties = ModelTransform(modelName = "pip__TestTrainSplit_0_out2")
    )
    (
        customer_csv_1._out(0)
        >> [pip__testtrainsplit_0_out0._in(0), pip__testtrainsplit_0_out1._in(0), pip__testtrainsplit_0_out2._in(0)]
    )
