import apache_beam as beam

p1 = beam.Pipeline()

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


customers = (
        p1
        | beam.io.ReadFromText('resource\myntraDatasheet.csv', skip_header_lines=True)
        | beam.ParDo(RemovingBrandNames(), side_list)
        | beam.ParDo(ProcessCustomers(), brand='Roadster', start_char='Women').with_outputs('Roadster_Data',
                                                                                            'Women_Data',
                                                                                            main='Excluded_Data')
)

Roadster_Data = customers.Roadster_Data
Data_after_exclusion = customers.Excluded_Data
Women_Data = customers.Women_Data

Roadster_Data | 'Roadster Data list' >> beam.io.WriteToText('resource/output/roadster_data_myntra')
Data_after_exclusion | 'Data after Exclusion list' >> beam.io.WriteToText("resource/output/Data_after_exclusion_myntra")
Women_Data | 'Womens data list' >> beam.io.WriteToText("resource/output/women_data_myntra")

p1.run()
