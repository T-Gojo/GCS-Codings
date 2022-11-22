import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions(
    save_main_session=True,
    # runner='Directrunner',
    # runner='Dataflowrunner',
    project='practice-id1',
    # temp_location='gs://runner_temp/tmp',
    region='US'
)
p1 = beam.Pipeline(options=beam_options)

table_schema = 'PRODUCT_ID:INTEGER, BRAND_NAME:STRING, CATEGORY:STRING, INDIVIDUAL_CATEGORY:STRING,' \
               'CATEGORY_BY_GENDER:STRING,' \
               'DESCRIPTION:STRING, DISCOUNT_PRICE:INTEGER, ORIGINAL_PRICE:INTEGER, DISCOUNT_OFFER:STRING,' \
               'RATINGS:FLOAT, REVIEWS:INTEGER'

side_list = list()
with open('resource\exclude_ids.txt', 'r') as my_file:
    for line in my_file:
        side_list.append(line.rstrip())


class RemovingBrandNames(beam.DoFn):
    def process(self, element, side_list):
        # brandName = element.split(',')[1]
        element_list = element.split(',')
        if element_list[1] not in side_list:
            return [element_list]


class ProcessCustomers(beam.DoFn):
    def process(self, element, brand, start_char):
        if element[1] == brand:
            yield beam.pvalue.TaggedOutput('Roadster_Data', element)
        else:
            yield element
        if element[4].startswith('Women'):
            yield beam.pvalue.TaggedOutput('Women_Data', element)


class json_conversion(beam.DoFn):
    def process(self, element):
        return [{'PRODUCT_ID': element[0], 'BRAND_NAME': element[1], 'CATEGORY': element[2],
                 'INDIVIDUAL_CATEGORY': element[3],
                 'CATEGORY_BY_GENDER': element[4], 'DESCRIPTION': element[5], 'DISCOUNT_PRICE': element[6],
                 'ORIGINAL_PRICE': element[7], 'DISCOUNT_OFFER': element[8],
                 'RATINGS': element[9], 'REVIEWS': element[10]}]


def run():
    customers = (
            p1
            | beam.io.ReadFromText('resource\smallmyntraDatasheet.csv', skip_header_lines=True)
            | beam.ParDo(RemovingBrandNames(), side_list)
            | beam.ParDo(ProcessCustomers(), brand='Roadster', start_char='Women').with_outputs('Roadster_Data',
                                                                                                'Women_Data',
                                                                                                main='Excluded_Data')
    )

    Roadster_Data = customers.Roadster_Data
    # Data_after_exclusion = customers.Excluded_Data
    # Women_Data = customers.Women_Data

    (Roadster_Data
     | "json conversion" >> beam.ParDo(json_conversion())
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='practice-id1:sideInputOutput.input2Roadster',
                custom_gcs_temp_location='gs://runner_temp/tmp/loadToBqCustom',
                project='practice-id1',
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, #write never
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
     # | 'Roadster Data list' >> beam.io.WriteToText('resource/output/roadster_data_myntra')

     )

    # Data_after_exclusion | 'Data after Exclusion list' >> beam.io.WriteToText("resource/output/Data_after_exclusion_myntra")
    # Women_Data | 'Womens data list' >> beam.io.WriteToText("resource/output/women_data_myntra")
    p1.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
