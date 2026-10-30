from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "all", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    all__datacleansing_1 = Process(
        name = "all__DataCleansing_1",
        properties = ModelTransform(modelName = "all__DataCleansing_1")
    )
    customer_csv_1 = Process(
        name = "customer_csv_1",
        properties = SharepointSource(
          format = SharepointSource.CsvReadFormat(schema = "external_sources/all/customer_csv_1.yml"),
          compression = SharepointSource.Compression(kind = "uncompressed"),
          properties = SharepointSource.SharepointSourceInternal(filePath = "/customer.csv"),
          connector = "sharepoint_1"
        ),
        input_ports = None
    )
    customer_csv_1 >> all__datacleansing_1
