
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return re.findall(r'[\w\']+',element, re.UNICODE)

def run(argv = None, save_min_session = True):
    parser = argparse.ArgumentParser()
    parser.add_argment(
        '--input',
        dest = 'input',
        default = 'gs://dataflow-samples/shakespeare/kinglear.txt',
        help = 'Input file tp process.'
        )

    parser.add_argment(
        '--output',
        dest = 'output',
        default = 'gs://aurora-temp/counts.txt',
        required = True,
        help = 'output file to write result to.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options = pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(known_args.input)
        counts = (
            lines 
            | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            | 'PariWithOne' >> beam.Map(lambda x: (x,1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )

    def format_result(word, count):
        return '%s:%d'%(word,count)

    output = counts | 'Format' >> beam.MapTuple(format_result)
    output | 'Write' >> WriteToText(known_args.output)


if __name__ == ' __main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# input_file = 'gs://dataflow-samples/shakespeare/kinglear.txt'
# output_file = 'gs://my-bucket/counts.txt'


# beam_options = PipelineOptions(
#     runner = 'DataflowRunner',
#     project = 'jiayi_test1',
#     job_name = 'jiayi_job_test1',
#     temp_location = 'gs://my-bucket/temp'
# )

# pipeline = beam.Pipeline(options = beam_options)

# pipeline 
# |'ReadFile' >> beam.io.ReadFromText(input_file)
# |'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+',x))
# |'MakeCounts' >> beam.combiners.Count.PerElement()
# |'Mapping' >> beam.MapTuple(lambda word, count : '%s:%s' % (word, count))
# |'WriteOutput' >> beam.io.WriteToText(output_file)

# # class FormatAsTextFn(beam.DoFn):
# #     def process(self,element):
# #         word,count = element
# #         yield '%s:%s' % (word,count)

# # formatted = counts | beam.ParDo(FormatAsTextFn())
# with beam.Pipeline(...) as p:
#   [construction]
# # p.run() automatically called