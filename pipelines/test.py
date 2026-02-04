from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
args = PipelineArgs(label = "test", version = 1, auto_layout = False)

with Pipeline(args) as pipeline:
    xyzart_csv_0 = Process(
        name = "XyzArt_csv_0",
        properties = S3Source(
          compression = S3Source.Compression(kind = "uncompressed"),
          connector = "s3",
          format = S3Source.CsvReadFormat(schema = "external_sources/test/XyzArt_csv_0.yml"),
          properties = S3Source.S3SourceInternal(filePath = "/XyzArt.csv")
        ),
        input_ports = None
    )
    test__xyzart_schema_info = Process(
        name = "test__xyzart_schema_info",
        properties = ModelTransform(modelName = "test__xyzart_schema_info")
    )
    test__datacleansing_1 = Process(
        name = "test__DataCleansing_1",
        properties = ModelTransform(modelName = "test__DataCleansing_1")
    )
    xyzart_csv_0._out(0) >> [test__xyzart_schema_info._in(0), test__datacleansing_1._in(0)]
